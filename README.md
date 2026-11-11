# StormChase scheduled publisher

This repository is separate from the StormChase desktop app. The GitHub Actions
workflow runs one publisher pass every five minutes and can also be started
manually from the Actions tab. Scheduled workflows are best-effort; GitHub may
delay or skip a run, so this is not a guaranteed always-on service.

## Before enabling scheduled runs

1. In the StormChase app repository, run the complete
   `supabase/001_stormchase_schema.sql` file in the Supabase SQL Editor. It
   configures two-hour live-row retention and a five-minute `pg_cron` cleanup.
   The publisher independently removes expired radar files through the
   Supabase Storage API.
2. Push this repository's `publisher.py`, `requirements.txt`, and
   `.github/workflows/publish.yml` to its default branch.
3. In this repository, open **Settings → Secrets and variables → Actions**.
   Add this repository variable:
   - `STORMCHASE_SOURCE_REPOSITORY`: app repository in `OWNER/REPOSITORY` form.
4. Add these Actions secrets:
   - `SUPABASE_URL`: the HTTPS project URL.
   - `SUPABASE_SERVICE_ROLE_KEY`: the server-side service-role key. Never put
     this key in the desktop app, app repository, or a public file.
   - `STORMCHASE_SOURCE_TOKEN`: only needed if the app source repository is
     private; use a fine-grained token with read-only **Contents** permission
     for that repository.
5. From **Actions → Publish StormChase data → Run workflow**, run a manual
   test. Inspect the logs for successful writes to all data products and for
   the radar Storage cleanup count.
6. Confirm the desktop app uses `STORMCHASE_SUPABASE_URL` and
   `STORMCHASE_SUPABASE_ANON_KEY`. The app uses the public anon key, never the
   service-role key.

The workflow checks out the app source to reuse its `radar.py`, `tracking.py`,
and `lightning.py` parsing and detection code. `STORMCHASE_SOURCE_REPOSITORY`
must point to the repository root where those modules live.

## Retention scope

Live radar metadata, live snapshots, and lightning observations are hidden from
clients after two hours and removed from their tables by the Supabase cron job.
Radar HDF5 objects are deleted by the publisher using the Storage API; storage
objects must not be deleted with SQL. Cleanup occurs on a five-minute cadence,
so file deletion may happen a few minutes after the two-hour cutoff.

Community `storm_reports` are user-submitted and are intentionally not deleted.
