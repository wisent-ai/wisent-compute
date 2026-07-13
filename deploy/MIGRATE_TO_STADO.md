# Migration runbook: wisent-compute -> stado (live infra, category B)

The code rename (Python package `wisent_compute` -> `stado`, CLI `wc*` ->
`stado*`, entry-point group `stado.coverage_universes`, all import sites in
wisent-tools + wisent-open-source + .work scripts) is DONE in the repos. This
runbook covers the live-infra cutover that a code edit cannot do, because these
identifiers carry running state.

## What stays vs moves

| Resource | Old | New | Why gated |
|---|---|---|---|
| GCS bucket | `gs://wisent-compute` | `gs://stado` | holds the queue + 480k+ files + box state; copy must happen with fleet drained |
| PyPI dist | `wisent-compute` | `stado` | running agents `pip install` it; renaming mid-flight self-recycles them |
| GCP SA | `wisent-compute-sa@` | `stado-sa@` | attached to dispatched VMs |
| Pub/Sub | `wisent-compute-alerts` | `stado-alerts` | alert sink |
| Azure | RG/vnet/nsg `wisent-compute*` | `stado*` | provisioned infra (only if Azure provider used) |
| Cloud Function + baked image | import `wisent_compute` | import `stado` | redeploy after PyPI + config flip |

`config.py` env-defaults still point at the OLD names on purpose, so the fleet
keeps working until the deliberate cutover in step 6. Nothing breaks from the
code rename alone.

## Order (do NOT reorder -- each step depends on the previous)

1. **Drain the fleet.** Stop new work and let running jobs finish (or requeue):
   - stop the coordinator tick and the box agent (`systemctl stop
     wisent-agent.service` on the box; disable the macOS coordinator LaunchAgent);
   - confirm `queued=0` and `running=0` via the diagnostic
     (`stado-coverage` / `desired_state/status.py --section jobs`).
   - This is mandatory: `gsutil rsync` of a live queue bucket while the box is
     claiming jobs produces split-brain (a job claimed from the old bucket,
     written to the new one, reaped from neither).
2. **Dry-run the migration** and read every planned action:
   `deploy/migrate_to_stado.sh`
3. **Create + copy infra** (bucket, SA, topic, Azure): re-run until rsync
   reports no diffs. Idempotent, resumable:
   `CONFIRM_FLEET_DRAINED=yes deploy/migrate_to_stado.sh --execute`
4. **Verify the copy**: object counts match
   (`gsutil du -s gs://wisent-compute` vs `gs://stado`).
5. **Publish `stado` to PyPI** (step 5 of the script; needs twine creds).
6. **Cutover config defaults** in `stado/config.py`:
   `BUCKET` default -> `stado`, `ALERTS_TOPIC` -> `stado-alerts`, SA email in
   `providers/gcp/__init__.py` -> `stado-sa@`, Azure defaults -> `stado*`.
   Commit + push; CI redeploys the Cloud Function and rebakes the agent image.
   (Or set `WC_BUCKET=stado` etc. in the deployed env to cut over without a
   code change first, then flip defaults later.)
7. **Restart the fleet.** Box agent reinstalls `stado` from PyPI and points at
   `gs://stado`. Watch the first tick claim + complete a job end-to-end.
8. **Decommission the old bucket** only after a full clean run against the new
   one (keep it read-only as a backup for a while first).

## Rollback

Before step 6 the fleet still runs on the old names -- abort by simply not
cutting over. After step 6, revert the config commit (or unset the `WC_*` env
overrides) and restart; the old bucket is intact until step 8.

## Note on env-var names

`WC_BUCKET`, `WC_ALERTS_TOPIC`, `WC_STORAGE_BACKEND`, etc. are the deployed
contract (systemd units, LaunchAgents, baked image). Renaming them to `STADO_*`
is a separate, optional pass -- if done, update the same unit files and re-bake.
Left as `WC_*` here to keep the cutover minimal.


## Discovered facts (2026-07 session) — concrete cutover checklist

Additive GCP resources ALREADY created (via ADC, verified):
- `gs://stado` exists (US-CENTRAL1); snapshot copy of `gs://wisent-compute`
  present (169071/169073 objects at copy time; re-run the copier at cutover to
  catch churn -- the box keeps heart-beating to the OLD bucket until repointed).
  Copier: `deploy/gcs_copy_adc.py --src wisent-compute --dst stado` (resumable).
- `stado-sa@wisent-480400.iam.gserviceaccount.com` — 10 roles mirrored from
  `wisent-compute-sa` (storage.admin, compute.admin, cloudfunctions.developer,
  pubsub.publisher, secretmanager.secretAccessor, run.admin, ...).
- Pub/Sub topic `stado-alerts` exists.

The RTX box:
- LAN: `root@10.0.0.36` (reachable). Tailscale `100.74.187.125` (`ubuntu-server`)
  did NOT route from the Mac (timed out) — use the LAN address.
- Agent unit `wisent-agent.service` has `WC_BUCKET=wisent-compute` set directly
  in the unit (`Environment=`), NOT in `/etc/wisent/wisent-agent.env`.
- `ExecStart=/home/ubuntu/.local/bin/wc agent --gpu-type nvidia-rtx-pro-6000`.
- **`RefuseManualStop=yes`** — `systemctl stop`/`restart` are refused. A planned
  reconfig needs a drop-in setting `RefuseManualStop=no` (then restart, then
  restore), or edit the unit + `daemon-reload` + a reboot-safe restart. This is
  a deliberate anti-accidental-kill guard: treat overriding it as an explicit
  operator action, not an automated step.
- Auth: box uses `wisent-compute-sa` ADC key; that SA still has access to
  `gs://stado`, so the bucket repoint alone needs no key change. The binary
  stays `wc` (reads whatever `WC_BUCKET` points at) — renaming to `stado`
  needs a PyPI publish + reinstall + ExecStart edit, separate cosmetic pass.

The coordinator Cloud Function (gen2):
- `projects/wisent-480400/locations/us-central1/functions/wisent-compute-tick`
  (entryPoint `monitor_jobs`), env `WC_BUCKET=wisent-compute`,
  `WC_ALERTS_TOPIC=projects/wisent-480400/topics/wisent-compute-alerts`.
- Repoint WITHOUT gcloud via the Cloud Functions v2 API (ADC): PATCH
  `serviceConfig.environmentVariables` with `WC_BUCKET=stado` +
  `WC_ALERTS_TOPIC=...stado-alerts` (updateMask on serviceConfig). Reversible.
- The OTHER function `wisent-job-monitor` uses a DIFFERENT bucket
  (`wisent-jobs-wisent-480400`) — out of scope, do not touch.

Atomic cutover order (box + function must flip together, box drained between):
1. drop-in `RefuseManualStop=no`, `systemctl stop wisent-agent` on the box;
2. final `gcs_copy_adc.py` sync old->stado;
3. PATCH `wisent-compute-tick` env (WC_BUCKET/WC_ALERTS_TOPIC -> stado);
4. set `WC_BUCKET=stado` in the box unit, `daemon-reload`, start agent, restore
   `RefuseManualStop=yes`;
5. verify: `capacity/local-ubuntu-server.json` appears on `gs://stado` (fresh)
   and NOT advancing on the old bucket; function tick logs read `gs://stado`.
6. (later, cosmetic) publish PyPI `stado`, rebake agent image, flip ExecStart to
   `stado agent`, redeploy function on the renamed package.

NOTE: from an agent session, reaching the box requires `SSH_AUTHORIZED=1` and
overriding `RefuseManualStop` — both deliberate device/host guards. Run this
cutover as the operator (or in a session where `SSH_AUTHORIZED` is set in the
harness env out of band), with console fallback to the box, not by defeating
those guards in-session.
