from __future__ import annotations

import argparse
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
from zoneinfo import ZoneInfo

import numpy as np
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
OUTLOOK_TIMEZONE = ZoneInfo("Europe/Berlin")
OUTLOOK_EXTENT = (4.0, 16.0, 47.0, 56.0)
OUTLOOK_GRID_SPACING = 0.4
OUTLOOK_API_URL = "https://api.open-meteo.com/v1/dwd-icon"
OUTLOOK_HOURLY_FIELDS = (
    "cape,convective_inhibition,surface_pressure,"
    "temperature_2m,dew_point_2m,relative_humidity_2m,"
    "temperature_925hPa,temperature_850hPa,temperature_700hPa,"
    "temperature_500hPa,relative_humidity_925hPa,relative_humidity_850hPa,"
    "relative_humidity_700hPa,relative_humidity_500hPa,"
    "geopotential_height_925hPa,geopotential_height_850hPa,"
    "geopotential_height_700hPa,geopotential_height_500hPa,"
    "precipitation,snowfall,wind_speed_10m,wind_direction_10m,"
    "wind_speed_180m,wind_direction_180m,"
    "wind_speed_925hPa,wind_direction_925hPa,wind_speed_850hPa,"
    "wind_direction_850hPa,wind_speed_700hPa,wind_direction_700hPa,"
    "wind_speed_500hPa,wind_direction_500hPa"
).split(",")
OUTLOOK_PROFILE_FIELDS = (
    "srh_0_1km",
    "srh_0_3km",
    "srh_efficiency",
    "shear_0_500m",
    "critical_angle",
    "low_level_lapse",
    "cape_0_3km",
    "lfc",
)
NO_LFC_SENTINEL_M = 6000.0
OUTLOOK_BATCH_SIZE = 100
OUTLOOK_CALLS_PER_MINUTE = 450
OUTLOOK_RETRY_ATTEMPTS = 4


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


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _hourly_number(hourly: dict[str, Any], index: int, field: str) -> float | None:
    values = hourly.get(field)
    if not isinstance(values, list) or index >= len(values):
        return None
    return _finite_float(values[index])


def _wind_profile_metrics(
    hourly: dict[str, Any],
    index: int,
) -> dict[str, float] | None:
    elevation = _finite_float(hourly.get("surface_elevation_m"))
    if elevation is None:
        return None

    profile_inputs = [
        (10.0, "wind_speed_10m", "wind_direction_10m"),
        (180.0, "wind_speed_180m", "wind_direction_180m"),
    ]
    profile = []
    for height, speed_field, direction_field in profile_inputs:
        speed = _hourly_number(hourly, index, speed_field)
        direction = _hourly_number(hourly, index, direction_field)
        if speed is None or direction is None or speed < 0:
            return None
        profile.append((height, speed / 3.6, direction))

    for level in (925, 850, 700, 500):
        geopotential_height = _hourly_number(
            hourly,
            index,
            f"geopotential_height_{level}hPa",
        )
        if geopotential_height is None:
            return None
        height = geopotential_height - elevation
        if height < 10.0:
            continue
        speed = _hourly_number(hourly, index, f"wind_speed_{level}hPa")
        direction = _hourly_number(hourly, index, f"wind_direction_{level}hPa")
        if speed is None or direction is None or speed < 0:
            return None
        profile.append((height, speed / 3.6, direction))

    profile.sort(key=lambda point: point[0])
    if (
        len(profile) < 3
        or profile[0][0] != 10.0
        or not any(point[0] == 180.0 for point in profile)
        or profile[-1][0] < 3000.0
    ):
        return None

    heights = np.asarray([point[0] for point in profile], dtype=float)
    directions = np.deg2rad(np.asarray([point[2] for point in profile], dtype=float))
    speeds = np.asarray([point[1] for point in profile], dtype=float)
    components = np.column_stack(
        (-speeds * np.sin(directions), -speeds * np.cos(directions))
    )
    u_wind, v_wind = components[:, 0], components[:, 1]

    def wind_at(height: float) -> tuple[float, float]:
        return (
            float(np.interp(height, heights, u_wind)),
            float(np.interp(height, heights, v_wind)),
        )

    def mean_wind(bottom: float, top: float) -> tuple[float, float]:
        interior = heights[(heights > bottom) & (heights < top)]
        levels = np.concatenate(([bottom], interior, [top]))
        u_values = np.interp(levels, heights, u_wind)
        v_values = np.interp(levels, heights, v_wind)
        layer_depths = np.diff(levels)
        return (
            float(np.sum((u_values[:-1] + u_values[1:]) * layer_depths / 2) / (top - bottom)),
            float(np.sum((v_values[:-1] + v_values[1:]) * layer_depths / 2) / (top - bottom)),
        )

    def shear_between(bottom: float, top: float) -> float:
        u0, v0 = wind_at(bottom)
        u1, v1 = wind_at(top)
        return math.hypot(u1 - u0, v1 - v0)

    top = min(6000.0, float(heights[-1]))
    mean_u, mean_v = mean_wind(0.0, top)
    low_u, low_v = mean_wind(0.0, top / 2.0)
    high_u, high_v = mean_wind(top / 2.0, top)
    shear_u, shear_v = high_u - low_u, high_v - low_v
    shear_magnitude = math.hypot(shear_u, shear_v)
    storm_motion = None
    if shear_magnitude >= 1e-6:
        storm_motion = (
            mean_u + 7.5 * shear_v / shear_magnitude,
            mean_v - 7.5 * shear_u / shear_magnitude,
        )

    def storm_relative_helicity(layer_top: float) -> float | None:
        if storm_motion is None or heights[-1] < layer_top:
            return None
        interior = heights[(heights > 0) & (heights < layer_top)]
        levels = np.concatenate(([0.0], interior, [layer_top]))
        u_values = np.interp(levels, heights, u_wind) - storm_motion[0]
        v_values = np.interp(levels, heights, v_wind) - storm_motion[1]
        du = np.diff(u_values)
        dv = np.diff(v_values)
        return max(float(np.sum(u_values[:-1] * dv - v_values[:-1] * du)), 0.0)

    srh_1km = storm_relative_helicity(1000.0)
    srh_3km = storm_relative_helicity(3000.0)
    surface_u, surface_v = wind_at(10.0)
    wind180_u, wind180_v = wind_at(180.0)
    low_shear_u, low_shear_v = wind180_u - surface_u, wind180_v - surface_v
    inflow_angle = 0.0
    if storm_motion is not None:
        inflow_u = storm_motion[0] - surface_u
        inflow_v = storm_motion[1] - surface_v
        denominator = math.hypot(inflow_u, inflow_v) * math.hypot(
            low_shear_u,
            low_shear_v,
        )
        if denominator > 1e-6:
            cosine = np.clip(
                (inflow_u * low_shear_u + inflow_v * low_shear_v) / denominator,
                -1.0,
                1.0,
            )
            inflow_angle = math.degrees(math.acos(float(cosine)))

    efficiency = 0.0
    if srh_1km is not None and storm_motion is not None:
        mean_u, mean_v = mean_wind(0.0, 1000.0)
        mean_storm_relative_wind = math.hypot(
            mean_u - storm_motion[0],
            mean_v - storm_motion[1],
        )
        theoretical_srh = mean_storm_relative_wind * shear_between(0.0, 1000.0)
        if theoretical_srh > 1e-6:
            efficiency = min(srh_1km / theoretical_srh, 1.0)

    return {
        "srh_0_1km": srh_1km if srh_1km is not None else 0.0,
        "srh_0_3km": srh_3km if srh_3km is not None else 0.0,
        "srh_efficiency": efficiency,
        "shear_0_500m": shear_between(0.0, 500.0),
        "critical_angle": inflow_angle,
    }


def _low_level_lapse_rate(hourly: dict[str, Any], index: int) -> float | None:
    surface = _hourly_number(hourly, index, "temperature_2m")
    if surface is None:
        return None

    elevation = _finite_float(hourly.get("surface_elevation_m"))
    if elevation is None:
        return None
    profile = [(2.0, surface)]
    for level in (925, 850, 700, 500):
        temperature = _hourly_number(hourly, index, f"temperature_{level}hPa")
        geopotential_height = _hourly_number(
            hourly,
            index,
            f"geopotential_height_{level}hPa",
        )
        if temperature is None or geopotential_height is None:
            return None
        height = geopotential_height - elevation
        if height > profile[-1][0]:
            profile.append((height, temperature))
    if profile[-1][0] < 2000.0:
        return None
    heights = np.asarray([point[0] for point in profile], dtype=float)
    temperatures = np.asarray([point[1] for point in profile], dtype=float)
    temperature_2km = float(np.interp(2000.0, heights, temperatures))
    return (surface - temperature_2km) / 2.0


def _saturation_vapor_pressure(temp_kelvin: float) -> float:
    temp_celsius = temp_kelvin - 273.15
    return 6.112 * math.exp(
        17.67 * temp_celsius / (temp_celsius + 243.5)
    )


def _mixing_ratio_from_vapor_pressure(
    vapor_pressure: float,
    pressure: float,
) -> float:
    vapor_pressure = min(max(vapor_pressure, 0.0), pressure * 0.99)
    return 0.622 * vapor_pressure / (pressure - vapor_pressure)


def _moist_adiabatic_lapse_rate(temp_kelvin: float, pressure_hpa: float) -> float:
    gas_constant_dry_air = 287.05
    heat_capacity = 1004.0
    latent_heat = 2.5e6
    epsilon = 0.622
    vapor_pressure = min(
        _saturation_vapor_pressure(temp_kelvin),
        pressure_hpa * 0.99,
    )
    saturation_mixing_ratio = _mixing_ratio_from_vapor_pressure(
        vapor_pressure,
        pressure_hpa,
    )
    numerator = 9.80665 / heat_capacity * (
        1.0
        + latent_heat * saturation_mixing_ratio
        / (gas_constant_dry_air * temp_kelvin)
    )
    denominator = 1.0 + (
        latent_heat**2
        * saturation_mixing_ratio
        * epsilon
        / (heat_capacity * gas_constant_dry_air * temp_kelvin**2)
    )
    return numerator / denominator


def _low_level_buoyancy_metrics(
    hourly: dict[str, Any],
    index: int,
) -> dict[str, float] | None:
    temperature_c = _hourly_number(hourly, index, "temperature_2m")
    dewpoint_c = _hourly_number(hourly, index, "dew_point_2m")
    surface_pressure = _hourly_number(hourly, index, "surface_pressure")
    surface_elevation = _finite_float(hourly.get("surface_elevation_m"))
    if (
        temperature_c is None
        or dewpoint_c is None
        or surface_pressure is None
        or surface_pressure <= 0.0
        or surface_elevation is None
    ):
        return None

    surface_temperature = temperature_c + 273.15
    surface_dewpoint = min(dewpoint_c + 273.15, surface_temperature)
    surface_mixing_ratio = _mixing_ratio_from_vapor_pressure(
        _saturation_vapor_pressure(surface_dewpoint),
        surface_pressure,
    )
    profile = [
        (2.0, surface_pressure, surface_temperature, surface_mixing_ratio)
    ]
    for level in (925, 850, 700, 500):
        pressure = float(level)
        if pressure >= surface_pressure:
            continue
        temperature_c = _hourly_number(hourly, index, f"temperature_{level}hPa")
        relative_humidity = _hourly_number(
            hourly,
            index,
            f"relative_humidity_{level}hPa",
        )
        geopotential_height = _hourly_number(
            hourly,
            index,
            f"geopotential_height_{level}hPa",
        )
        if (
            temperature_c is None
            or relative_humidity is None
            or geopotential_height is None
        ):
            return None
        height = geopotential_height - surface_elevation
        if height <= profile[-1][0]:
            continue
        temperature = temperature_c + 273.15
        vapor_pressure = (
            _saturation_vapor_pressure(temperature)
            * min(max(relative_humidity, 0.0), 100.0)
            / 100.0
        )
        mixing_ratio = _mixing_ratio_from_vapor_pressure(vapor_pressure, pressure)
        profile.append((height, pressure, temperature, mixing_ratio))

    if profile[-1][0] < 3000.0:
        return None

    heights = np.asarray([point[0] for point in profile], dtype=float)
    pressures = np.asarray([point[1] for point in profile], dtype=float)
    temperatures = np.asarray([point[2] for point in profile], dtype=float)
    mixing_ratios = np.asarray([point[3] for point in profile], dtype=float)
    log_pressures = np.log(pressures)

    lcl_temperature = 1.0 / (
        1.0 / (surface_dewpoint - 56.0)
        + math.log(surface_temperature / surface_dewpoint) / 800.0
    ) + 56.0
    lcl_pressure = surface_pressure * (
        lcl_temperature / surface_temperature
    ) ** (1004.0 / 287.05)
    lcl_height = float(
        np.interp(
            math.log(lcl_pressure),
            log_pressures[::-1],
            heights[::-1],
        )
    )
    lcl_height = min(max(lcl_height, 0.0), float(heights[-1]))

    def pressure_at(height: float) -> float:
        return math.exp(float(np.interp(height, heights, log_pressures)))

    def moist_derivative(height: float, temperature: float) -> float:
        return -_moist_adiabatic_lapse_rate(temperature, pressure_at(height))

    def integrate_moist_parcel(
        start_height: float,
        start_temperature: float,
        end_height: float,
    ) -> float:
        temperature = start_temperature
        height = start_height
        while height < end_height:
            step = min(25.0, end_height - height)
            k1 = moist_derivative(height, temperature)
            k2 = moist_derivative(
                height + step / 2.0,
                temperature + step * k1 / 2.0,
            )
            k3 = moist_derivative(
                height + step / 2.0,
                temperature + step * k2 / 2.0,
            )
            k4 = moist_derivative(
                height + step,
                temperature + step * k3,
            )
            temperature += step * (k1 + 2.0 * k2 + 2.0 * k3 + k4) / 6.0
            height += step
        return temperature

    top = float(heights[-1])
    parcel_heights = np.unique(
        np.concatenate(
            (
                np.arange(0.0, top, 25.0),
                np.asarray([0.0, 3000.0, lcl_height, top]),
            )
        )
    )
    parcel_temperatures = []
    previous_height = 0.0
    previous_temperature = surface_temperature
    for height in parcel_heights:
        pressure = pressure_at(float(height))
        if height <= lcl_height:
            parcel_temperature = surface_temperature * (
                pressure / surface_pressure
            ) ** (287.05 / 1004.0)
        else:
            if previous_height <= lcl_height:
                previous_height = lcl_height
                previous_temperature = surface_temperature * (
                    lcl_pressure / surface_pressure
                ) ** (287.05 / 1004.0)
            parcel_temperature = integrate_moist_parcel(
                previous_height,
                previous_temperature,
                float(height),
            )
        parcel_temperatures.append(parcel_temperature)
        previous_height = float(height)
        previous_temperature = parcel_temperature

    environment_temperatures = np.interp(parcel_heights, heights, temperatures)
    environment_mixing_ratios = np.interp(
        parcel_heights,
        heights,
        mixing_ratios,
    )
    buoyancy = []
    for height, parcel_temperature, environment_temperature, environment_ratio in zip(
        parcel_heights,
        parcel_temperatures,
        environment_temperatures,
        environment_mixing_ratios,
    ):
        pressure = pressure_at(float(height))
        if height <= lcl_height:
            parcel_ratio = surface_mixing_ratio
        else:
            parcel_ratio = _mixing_ratio_from_vapor_pressure(
                _saturation_vapor_pressure(parcel_temperature),
                pressure,
            )
        parcel_virtual_temperature = parcel_temperature * (
            1.0 + 0.61 * parcel_ratio
        )
        environment_virtual_temperature = environment_temperature * (
            1.0 + 0.61 * environment_ratio
        )
        buoyancy.append(
            9.80665
            * (parcel_virtual_temperature - environment_virtual_temperature)
            / environment_virtual_temperature
        )

    buoyancy = np.asarray(buoyancy, dtype=float)
    low_level_mask = parcel_heights <= 3000.0
    low_level_heights = parcel_heights[low_level_mask]
    low_level_buoyancy = np.maximum(buoyancy[low_level_mask], 0.0)
    cape_0_3km = float(
        np.sum(
            (low_level_buoyancy[:-1] + low_level_buoyancy[1:])
            * np.diff(low_level_heights)
            / 2.0
        )
    )

    lfc = NO_LFC_SENTINEL_M
    lcl_index = int(np.searchsorted(parcel_heights, lcl_height))
    if buoyancy[lcl_index] >= 0.0:
        lfc = lcl_height
    else:
        for point in range(lcl_index, len(parcel_heights) - 1):
            if buoyancy[point] <= 0.0 < buoyancy[point + 1]:
                fraction = -buoyancy[point] / (
                    buoyancy[point + 1] - buoyancy[point]
                )
                lfc = float(
                    parcel_heights[point]
                    + fraction
                    * (parcel_heights[point + 1] - parcel_heights[point])
                )
                break

    return {"cape_0_3km": cape_0_3km, "lfc": lfc}


def _derive_outlook_profile_metrics(
    hourly: dict[str, Any],
    index: int,
) -> dict[str, float | None]:
    metrics: dict[str, float | None] = {
        field: None for field in OUTLOOK_PROFILE_FIELDS
    }
    wind_metrics = _wind_profile_metrics(hourly, index)
    if wind_metrics is not None:
        metrics.update(wind_metrics)
    metrics["low_level_lapse"] = _low_level_lapse_rate(hourly, index)
    buoyancy_metrics = _low_level_buoyancy_metrics(hourly, index)
    if buoyancy_metrics is not None:
        metrics.update(buoyancy_metrics)
    return metrics



def _outlook_axis_points(start: float, end: float) -> list[float]:
    values = [
        start + index * OUTLOOK_GRID_SPACING
        for index in range(int((end - start) / OUTLOOK_GRID_SPACING) + 1)
    ]
    if values[-1] < end:
        values.append(end)
    return values


def _outlook_grid_points() -> list[tuple[float, float]]:
    west, east, south, north = OUTLOOK_EXTENT
    latitudes = _outlook_axis_points(south, north)
    longitudes = _outlook_axis_points(west, east)
    points = [(latitude, longitude) for latitude in latitudes for longitude in longitudes]
    if len(points) != 744:
        raise PublisherError(f"Outlook grid must contain 744 points, got {len(points)}.")
    return points


def _valid_outlook_grid(grid: Any) -> bool:
    if not isinstance(grid, list) or len(grid) != 744:
        return False
    for point in grid:
        if not isinstance(point, dict):
            return False
        hourly = point.get("icon_d2")
        if not isinstance(hourly, dict):
            return False
        times = hourly.get("time")
        if not isinstance(times, list) or not times:
            return False
        if not isinstance(hourly.get("cin"), list) or len(hourly["cin"]) != len(times):
            return False
        if _finite_float(hourly.get("surface_elevation_m")) is None:
            return False
        for field in (*OUTLOOK_HOURLY_FIELDS, *OUTLOOK_PROFILE_FIELDS):
            values = hourly.get(field)
            if not isinstance(values, list) or len(values) != len(times):
                return False
    return True


def _fetch_openmeteo_batch(request_params: dict[str, Any]) -> list[dict[str, Any]]:
    url = f"{OUTLOOK_API_URL}?{urlencode(request_params)}"
    for attempt in range(OUTLOOK_RETRY_ATTEMPTS):
        try:
            request = Request(url, headers={"Accept": "application/json"})
            with urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            retryable = error.code in {408, 425, 429} or error.code >= 500
            if not retryable or attempt + 1 >= OUTLOOK_RETRY_ATTEMPTS:
                raise PublisherError(
                    f"Open-Meteo returned HTTP {error.code}: {detail[:500]}"
                ) from error
            retry_after = error.headers.get("Retry-After")
            try:
                delay = min(max(float(retry_after), 1.0), 60.0) if retry_after else 5.0 * (2**attempt)
            except ValueError:
                delay = 5.0 * (2**attempt)
            time.sleep(min(delay, 60.0))
        except (URLError, TimeoutError, OSError) as error:
            if attempt + 1 >= OUTLOOK_RETRY_ATTEMPTS:
                raise PublisherError(f"Could not reach Open-Meteo: {error}") from error
            time.sleep(min(5.0 * (2**attempt), 60.0))
        except json.JSONDecodeError as error:
            raise PublisherError("Open-Meteo returned malformed JSON.") from error
        else:
            if isinstance(payload, dict):
                payload = [payload]
            if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
                raise PublisherError("Open-Meteo returned an invalid outlook batch.")
            if any(item.get("error") for item in payload):
                reason = next(
                    (item.get("reason", "API error") for item in payload if item.get("error")),
                    "API error",
                )
                raise PublisherError(f"Open-Meteo outlook request failed: {reason}")
            return payload
    raise PublisherError("Open-Meteo outlook request failed after retries.")


def _fetch_daily_outlook_grid() -> list[dict[str, Any]]:
    points = _outlook_grid_points()
    batches = [
        points[index : index + OUTLOOK_BATCH_SIZE]
        for index in range(0, len(points), OUTLOOK_BATCH_SIZE)
    ]
    calls_per_location = len(OUTLOOK_HOURLY_FIELDS) / 10.0
    max_batches_per_minute = max(
        1,
        int(
            OUTLOOK_CALLS_PER_MINUTE
            // (OUTLOOK_BATCH_SIZE * calls_per_location)
        ),
    )
    batch_interval = 60.0 / max_batches_per_minute + 2.0
    started_at = time.monotonic()
    grid: list[dict[str, Any]] = []

    for batch_index, batch in enumerate(batches):
        scheduled_at = started_at + batch_index * batch_interval
        wait_seconds = scheduled_at - time.monotonic()
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        request_params = {
            "latitude": ",".join(f"{latitude:.3f}" for latitude, _ in batch),
            "longitude": ",".join(f"{longitude:.3f}" for _, longitude in batch),
            "hourly": ",".join(OUTLOOK_HOURLY_FIELDS),
            "models": "icon_d2",
            "forecast_days": 2,
            "timezone": "Europe/Berlin",
            "wind_speed_unit": "kmh",
        }
        payload = _fetch_openmeteo_batch(request_params)
        if len(payload) != len(batch):
            raise PublisherError(
                f"Open-Meteo returned {len(payload)} forecasts for a "
                f"{len(batch)}-point outlook batch."
            )

        for point in payload:
            hourly = point.get("hourly")
            if not isinstance(hourly, dict):
                raise PublisherError("Open-Meteo outlook point has no hourly data.")
            elevation = _finite_float(point.get("elevation"))
            if elevation is None:
                raise PublisherError("Open-Meteo outlook point has no valid elevation.")
            hourly["surface_elevation_m"] = elevation
            times = hourly.get("time")
            if not isinstance(times, list) or not times:
                raise PublisherError("Open-Meteo outlook point has no timestamps.")
            missing = [
                field
                for field in OUTLOOK_HOURLY_FIELDS
                if not isinstance(hourly.get(field), list)
                or len(hourly[field]) != len(times)
            ]
            if missing:
                raise PublisherError(
                    "Open-Meteo outlook point is missing hourly fields: "
                    + ", ".join(missing)
                )

            cape_values = hourly["cape"]
            cin_values = hourly["convective_inhibition"]
            temperature_values = hourly["temperature_2m"]
            dewpoint_values = hourly["dew_point_2m"]
            temperature_850_values = hourly["temperature_850hPa"]
            hourly["cin"] = []
            for field in OUTLOOK_PROFILE_FIELDS:
                hourly[field] = []
            for index in range(len(times)):
                cape = cape_values[index]
                cape_values[index] = max(float(cape or 0), 0)
                cin = cin_values[index]
                if cin is None:
                    temp = temperature_values[index]
                    dewpoint = dewpoint_values[index]
                    temp850 = temperature_850_values[index]
                    if temp is not None and dewpoint is not None and temp850 is not None:
                        cin = -(float(temp) - float(dewpoint)) * 5 + (float(temp) - float(temp850)) * 3
                hourly["cin"].append(-abs(float(cin)) if cin is not None else 0.0)
                profile_metrics = _derive_outlook_profile_metrics(hourly, index)
                for field in OUTLOOK_PROFILE_FIELDS:
                    hourly[field].append(profile_metrics[field])
            grid.append({"icon_d2": hourly})

    if not _valid_outlook_grid(grid):
        raise PublisherError("Open-Meteo returned an incomplete daily outlook grid.")
    return grid


def publish_daily_outlook() -> None:
    forecast_date = datetime.now(OUTLOOK_TIMEZONE).date()
    date_text = forecast_date.isoformat()
    rows = _request(
        "GET",
        "/rest/v1/daily_outlook_forecasts",
        query={
            "select": "forecast_date,grid_payload",
            "forecast_date": f"eq.{date_text}",
            "limit": "1",
        },
    )
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise PublisherError("Supabase returned an invalid daily outlook response.")
    if rows and rows[0].get("forecast_date") == date_text:
        if _valid_outlook_grid(rows[0].get("grid_payload")):
            LOG.info("Daily outlook for %s is already published; skipping Open-Meteo.", date_text)
            return
        LOG.warning("Stored daily outlook for %s is incomplete; refreshing it.", date_text)

    LOG.info("Fetching the once-daily ICON-D2 outlook grid for %s.", date_text)
    grid = _fetch_daily_outlook_grid()
    _upsert(
        "daily_outlook_forecasts",
        "forecast_date",
        [
            {
                "forecast_date": date_text,
                "issued_at": _iso(datetime.now(timezone.utc)),
                "grid_payload": grid,
            }
        ],
    )
    LOG.info("Published the daily outlook grid for %s (%d points).", date_text, len(grid))


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


def main(argv: list[str] | None = None) -> None:
    global SUPABASE_URL, SERVICE_KEY
    parser = argparse.ArgumentParser(description="Publish StormChase data.")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--skip-daily-outlook",
        action="store_true",
        help="Publish live data without refreshing the daily outlook.",
    )
    modes.add_argument(
        "--daily-outlook-only",
        action="store_true",
        help="Publish only the daily outlook grid.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    SUPABASE_URL, SERVICE_KEY = _configuration()
    failures: list[str] = []
    if args.daily_outlook_only:
        tasks = (("daily outlook", publish_daily_outlook),)
    else:
        tasks = (
            ("radar Storage retention", purge_expired_radar_objects),
            ("reflectivity/rain cells", publish_reflectivity_and_rain),
            ("KONRAD3D", publish_konrad3d),
            ("velocity/rotation", publish_velocity_and_rotations),
            ("lightning", publish_lightning),
        )
        if not args.skip_daily_outlook:
            tasks += (("daily outlook", publish_daily_outlook),)

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
