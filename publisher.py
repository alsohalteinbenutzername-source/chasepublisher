from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import PurePosixPath
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

import websockets
from wetterdienst.provider.dwd.radar import (
    DwdRadarDate,
    DwdRadarParameter,
    DwdRadarValues,
)

from lightning import build_blitzortung_request, parse_blitzortung_message
from radar import (
    REFLECTIVITY_PRODUCT,
    VELOCITY_PRODUCT,
    decode_radar_file,
    detect_rain_cells,
    detect_rotations,
    velocity_radar_sites,
)
from tracking import fetch_latest_storm_cells


LOG = logging.getLogger("stormchase-publisher")
BUCKET = "stormchase-radar"
LIGHTNING_URLS = (
    "wss://live.lightningmaps.org:443/",
    "wss://live2.lightningmaps.org:443/",
)
RAIN_TRACK_MAX_GAP = timedelta(minutes=20)
RAIN_TRACK_MAX_DISTANCE_KM = 45.0
RAIN_TRACK_MAX_POINTS = 12
LIVE_DATA_RETENTION = timedelta(hours=2)
STORAGE_LIST_PAGE_SIZE = 1000
STORAGE_DELETE_BATCH_SIZE = 1000


class PublisherError(RuntimeError):
    pass


def _configuration() -> tuple[str, str]:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise PublisherError("SUPABASE_URL must be an HTTPS project URL.")
    if not key:
        raise PublisherError("SUPABASE_SERVICE_ROLE_KEY is not configured.")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise PublisherError("SUPABASE_URL must not contain credentials or a query.")
    return url, key


SUPABASE_URL = ""
SERVICE_KEY = ""


def _request(
    method: str,
    path: str,
    *,
    query: dict[str, str] | None = None,
    payload: Any = None,
    raw_body: bytes | None = None,
    content_type: str = "application/json",
    extra_headers: dict[str, str] | None = None,
) -> Any:
    if not SUPABASE_URL or not SERVICE_KEY:
        raise PublisherError("Supabase publisher credentials are not configured.")
    url = f"{SUPABASE_URL}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Accept": "application/json",
    }
    body = raw_body
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
        headers["Content-Type"] = "application/json"
    elif raw_body is not None:
        headers["Content-Type"] = content_type
    if extra_headers:
        headers.update(extra_headers)

    for attempt in range(4):
        try:
            request = Request(url, data=body, headers=headers, method=method)
            with urlopen(request, timeout=45) as response:
                contents = response.read()
            if not contents:
                return None
            try:
                return json.loads(contents)
            except json.JSONDecodeError:
                return contents
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            retryable = error.code in {408, 425, 429} or error.code >= 500
            if not retryable or attempt == 3:
                raise PublisherError(
                    f"Supabase {method} {path} returned HTTP {error.code}: "
                    f"{detail[:500]}"
                ) from error
        except URLError as error:
            if attempt == 3:
                raise PublisherError(
                    f"Could not reach Supabase for {method} {path}: {error.reason}"
                ) from error
        time.sleep(2**attempt)
    raise PublisherError(f"Supabase request failed: {method} {path}")


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _valid_timestamp(value: datetime | None, product: str) -> datetime:
    if value is None:
        raise PublisherError(f"DWD did not provide an observation time for {product}.")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _result_bytes(result: Any) -> bytes:
    data = result.data
    contents = data.getvalue() if hasattr(data, "getvalue") else data
    if not isinstance(contents, bytes) or not contents:
        raise PublisherError("DWD returned an empty or invalid radar file.")
    return contents


def _upsert(table: str, conflict: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    _request(
        "POST",
        f"/rest/v1/{table}",
        query={"on_conflict": conflict},
        payload=rows,
        extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
    )


def _publish_snapshot(product: str, scope: str, observed_at: datetime, payload: dict[str, Any]) -> None:
    _upsert(
        "live_snapshots",
        "product,scope,observed_at",
        [
            {
                "product": product,
                "scope": scope,
                "observed_at": _iso(observed_at),
                "payload": payload,
            }
        ],
    )


def _publish_radar(
    product: str,
    site_code: str,
    observed_at: datetime,
    contents: bytes,
) -> None:
    timestamp_name = observed_at.strftime("%Y%m%dT%H%M%SZ")
    folder = f"{product}/{site_code or 'composite'}/{observed_at:%Y/%m/%d}"
    storage_path = f"{folder}/{timestamp_name}.h5"
    encoded_path = "/".join(quote(part, safe="") for part in storage_path.split("/"))
    _request(
        "POST",
        f"/storage/v1/object/{BUCKET}/{encoded_path}",
        raw_body=contents,
        content_type="application/x-hdf5",
        extra_headers={"x-upsert": "true"},
    )
    _upsert(
        "radar_frames",
        "product,site_code,observed_at",
        [
            {
                "product": product,
                "site_code": site_code,
                "observed_at": _iso(observed_at),
                "storage_path": storage_path,
            }
        ],
    )


def _storage_list(prefix: str, offset: int) -> list[dict[str, Any]]:
    result = _request(
        "POST",
        f"/storage/v1/object/list/{BUCKET}",
        payload={
            "prefix": prefix,
            "limit": STORAGE_LIST_PAGE_SIZE,
            "offset": offset,
            "sortBy": {"column": "name", "order": "asc"},
        },
    )
    if not isinstance(result, list) or not all(
        isinstance(item, dict) for item in result
    ):
        raise PublisherError(f"Storage returned an invalid listing for {prefix!r}.")
    return result


def _collect_expired_radar_objects(
    prefix: str = "",
    *,
    now: datetime | None = None,
) -> list[str]:
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    cutoff = current_time.astimezone(timezone.utc) - LIVE_DATA_RETENTION

    expired: list[str] = []
    offset = 0
    while True:
        items = _storage_list(prefix, offset)
        for item in items:
            name = item.get("name")
            if not isinstance(name, str) or not name or "/" in name:
                raise PublisherError("Storage returned an invalid object name.")
            object_path = f"{prefix}/{name}" if prefix else name
            if item.get("id") is None:
                expired.extend(_collect_expired_radar_objects(object_path, now=current_time))
                continue

            parts = PurePosixPath(object_path).parts
            if (
                len(parts) != 6
                or parts[0] not in {"reflectivity", "velocity"}
                or (parts[0] == "reflectivity" and parts[1] != "composite")
                or (parts[0] == "velocity" and not parts[1])
                or not parts[2].isdigit()
                or not parts[3].isdigit()
                or not parts[4].isdigit()
                or not parts[5].endswith(".h5")
            ):
                continue
            try:
                observed_at = datetime.strptime(
                    parts[5][:-3],
                    "%Y%m%dT%H%M%SZ",
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if observed_at < cutoff:
                expired.append(object_path)

        if len(items) < STORAGE_LIST_PAGE_SIZE:
            break
        offset += len(items)
    return expired


def purge_expired_radar_objects() -> int:
    expired = _collect_expired_radar_objects()
    for start in range(0, len(expired), STORAGE_DELETE_BATCH_SIZE):
        batch = expired[start : start + STORAGE_DELETE_BATCH_SIZE]
        _request(
            "DELETE",
            f"/storage/v1/object/{BUCKET}",
            payload={"prefixes": batch},
        )
    LOG.info(
        "Removed %d radar Storage objects older than two hours.",
        len(expired),
    )
    return len(expired)


def _read_latest_rain_snapshot() -> tuple[datetime, list[dict[str, Any]]] | None:
    result = _request(
        "GET",
        "/rest/v1/live_snapshots",
        query={
            "select": "observed_at,payload",
            "product": "eq.rain_cells",
            "scope": "eq.",
            "order": "observed_at.desc",
            "limit": "1",
        },
    )
    if not isinstance(result, list) or not result:
        return None
    row = result[0]
    try:
        observed_at = datetime.fromisoformat(
            str(row["observed_at"]).replace("Z", "+00:00")
        )
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        payload = row["payload"]
        cells = payload["cells"]
    except (KeyError, TypeError, ValueError) as error:
        raise PublisherError("The latest stored rain-cell snapshot is malformed.") from error
    if not isinstance(cells, list):
        raise PublisherError("The latest stored rain-cell snapshot has no cells array.")
    return observed_at.astimezone(timezone.utc), cells


def _distance_km(first: tuple[float, float], second: tuple[float, float]) -> float:
    mean_latitude = math.radians((first[0] + second[0]) / 2)
    latitude_distance = (first[0] - second[0]) * 111.0
    longitude_distance = (
        (first[1] - second[1]) * 111.0 * math.cos(mean_latitude)
    )
    return math.hypot(latitude_distance, longitude_distance)


def _rain_payload(
    detected_cells: tuple[Any, ...],
    observed_at: datetime,
) -> dict[str, Any]:
    previous = _read_latest_rain_snapshot()
    previous_cells: list[dict[str, Any]] = []
    previous_time: datetime | None = None
    if previous is not None:
        previous_time, previous_cells = previous

    matches: dict[int, int] = {}
    if (
        previous_time is not None
        and timedelta(0) <= observed_at - previous_time <= RAIN_TRACK_MAX_GAP
    ):
        candidates: list[tuple[float, int, int]] = []
        for current_index, cell in enumerate(detected_cells):
            current_position = (cell.latitude, cell.longitude)
            for previous_index, old in enumerate(previous_cells):
                try:
                    old_position = (float(old["latitude"]), float(old["longitude"]))
                    distance = _distance_km(current_position, old_position)
                except (KeyError, TypeError, ValueError):
                    continue
                if distance <= RAIN_TRACK_MAX_DISTANCE_KM:
                    candidates.append((distance, current_index, previous_index))
        used_current: set[int] = set()
        used_previous: set[int] = set()
        for _distance, current_index, previous_index in sorted(candidates):
            if current_index in used_current or previous_index in used_previous:
                continue
            matches[current_index] = previous_index
            used_current.add(current_index)
            used_previous.add(previous_index)

    serialized: list[dict[str, Any]] = []
    for index, cell in enumerate(detected_cells, start=1):
        current_position = (cell.latitude, cell.longitude)
        previous_index = matches.get(index - 1)
        old = previous_cells[previous_index] if previous_index is not None else None
        if old is None:
            identifier = f"rain-{observed_at:%Y%m%dT%H%M%SZ}-{index:03d}"
            track: list[dict[str, Any]] = []
        else:
            identifier = str(old.get("identifier") or f"rain-{index:03d}")
            track = list(old.get("track") or [])
            if not track and previous_time is not None:
                track.append(
                    {
                        "latitude": float(old["latitude"]),
                        "longitude": float(old["longitude"]),
                        "timestamp": _iso(previous_time),
                        "reflectivity_dbz": float(old["max_reflectivity_dbz"]),
                    }
                )
            previous_position = (float(old["latitude"]), float(old["longitude"]))
            last_point_time = previous_time
            if track:
                try:
                    last_point_time = datetime.fromisoformat(
                        str(track[-1]["timestamp"]).replace("Z", "+00:00")
                    )
                    if last_point_time.tzinfo is None:
                        last_point_time = last_point_time.replace(tzinfo=timezone.utc)
                except (KeyError, TypeError, ValueError):
                    last_point_time = previous_time
            if (
                last_point_time is not None
                and observed_at > last_point_time
                and _distance_km(current_position, previous_position) >= 0.1
            ):
                track.append(
                    {
                        "latitude": cell.latitude,
                        "longitude": cell.longitude,
                        "timestamp": _iso(observed_at),
                        "reflectivity_dbz": cell.max_reflectivity_dbz,
                    }
                )
                track = track[-RAIN_TRACK_MAX_POINTS:]

        if not track:
            track = [
                {
                    "latitude": cell.latitude,
                    "longitude": cell.longitude,
                    "timestamp": _iso(observed_at),
                    "reflectivity_dbz": cell.max_reflectivity_dbz,
                }
            ]
        serialized.append(
            {
                "identifier": identifier,
                "latitude": cell.latitude,
                "longitude": cell.longitude,
                "max_reflectivity_dbz": cell.max_reflectivity_dbz,
                "area_pixels": cell.area_pixels,
                "track": track,
            }
        )
    return {"cells": serialized}


def publish_reflectivity_and_rain() -> None:
    request = DwdRadarValues(
        parameter=REFLECTIVITY_PRODUCT,
        start=DwdRadarDate.LATEST,
    )
    result = next(iter(request.query()), None)
    if result is None:
        LOG.warning("DWD has no latest reflectivity frame; skipping this product.")
        return
    observed_at = _valid_timestamp(
        result.timestamp or result.data_updated_at,
        "reflectivity",
    )
    contents = _result_bytes(result)
    frame = decode_radar_file(contents, timestamp=observed_at)
    _publish_radar("reflectivity", "", observed_at, contents)
    cells = detect_rain_cells(frame)
    _publish_snapshot("rain_cells", "", observed_at, _rain_payload(cells, observed_at))
    LOG.info("Published reflectivity and %d rain cells.", len(cells))


def publish_konrad3d() -> None:
    update = fetch_latest_storm_cells()
    if update.timestamp is None:
        LOG.warning("DWD has no timestamped KONRAD3D scan; skipping this product.")
        return
    observed_at = _valid_timestamp(update.timestamp, "KONRAD3D")
    cells = [
        {
            "identifier": cell.identifier,
            "latitude": cell.latitude,
            "longitude": cell.longitude,
            "reflectivity_dbz": cell.reflectivity_dbz,
            "severity": cell.severity,
            "track": [[latitude, longitude] for latitude, longitude in cell.track],
            "forecast": [
                {
                    "timestamp": _iso(timestamp),
                    "latitude": position[0],
                    "longitude": position[1],
                }
                for timestamp, position in cell.forecast
            ],
        }
        for cell in update.cells
    ]
    _publish_snapshot("konrad3d", "", observed_at, {"cells": cells})
    LOG.info("Published %d KONRAD3D cells.", len(cells))


def _fetch_velocity_result(site: Any) -> Any | None:
    request = DwdRadarValues(
        parameter=VELOCITY_PRODUCT,
        site=site,
        start=DwdRadarDate.LATEST,
    )
    return next(iter(request.query()), None)


def publish_velocity_and_rotations() -> None:
    sites = velocity_radar_sites()
    successful = 0
    for radar_site in sites:
        site_code = radar_site.site.value
        result = _fetch_velocity_result(radar_site.site)
        if result is None:
            LOG.warning("No current velocity frame for DWD site %s.", site_code)
            continue
        observed_at = _valid_timestamp(
            result.timestamp or result.data_updated_at,
            f"velocity at {site_code}",
        )
        contents = _result_bytes(result)
        frame = decode_radar_file(contents, timestamp=observed_at)
        _publish_radar("velocity", site_code, observed_at, contents)
        detections = detect_rotations(frame)
        _publish_snapshot(
            "rotation_detections",
            site_code,
            observed_at,
            {
                "detections": [
                    {
                        "latitude": detection.latitude,
                        "longitude": detection.longitude,
                        "negative_pixels": detection.negative_pixels,
                        "positive_pixels": detection.positive_pixels,
                    }
                    for detection in detections
                ]
            },
        )
        successful += 1
        LOG.info(
            "Published velocity for %s and %d rotation detections.",
            site_code,
            len(detections),
        )
    LOG.info("Published velocity for %d of %d active Doppler sites.", successful, len(sites))


async def _collect_lightning_for(seconds: float) -> dict[str, Any]:
    strokes: dict[str, Any] = {}
    errors: list[str] = []
    for url in LIGHTNING_URLS:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=10,
            ) as connection:
                await connection.send(build_blitzortung_request({}))
                deadline = time.monotonic() + seconds
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return strokes
                    try:
                        message = await asyncio.wait_for(
                            connection.recv(),
                            timeout=remaining,
                        )
                    except asyncio.TimeoutError:
                        return strokes
                    for strike in parse_blitzortung_message(message):
                        strokes[strike.identifier] = strike
        except Exception as error:
            errors.append(f"{url} ({type(error).__name__}: {error})")
            LOG.warning("Lightning feed connection failed at %s.", url)
    if errors and not strokes:
        raise PublisherError("Could not connect to either lightning feed: " + "; ".join(errors))
    return strokes


def publish_lightning() -> None:
    strikes = asyncio.run(_collect_lightning_for(15))
    rows = [
        {
            "identifier": strike.identifier,
            "latitude": strike.latitude,
            "longitude": strike.longitude,
            "observed_at": _iso(strike.timestamp),
            "quality": strike.quality,
            "uncertainty_km": strike.uncertainty_km,
        }
        for strike in strikes.values()
    ]
    _upsert("lightning_strikes", "identifier", rows)
    LOG.info("Published %d lightning strikes.", len(rows))


def main() -> None:
    global SUPABASE_URL, SERVICE_KEY
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    SUPABASE_URL, SERVICE_KEY = _configuration()
    failures: list[str] = []
    tasks = (
        ("radar Storage retention", purge_expired_radar_objects),
        ("reflectivity/rain cells", publish_reflectivity_and_rain),
        ("KONRAD3D", publish_konrad3d),
        ("velocity/rotation", publish_velocity_and_rotations),
        ("lightning", publish_lightning),
    )
    for name, task in tasks:
        try:
            task()
        except Exception as error:
            LOG.exception("Publishing %s failed.", name)
            failures.append(name)
    if failures:
        raise PublisherError("One or more publisher tasks failed: " + ", ".join(failures))
    LOG.info("Publisher run completed.")


if __name__ == "__main__":
    main()
