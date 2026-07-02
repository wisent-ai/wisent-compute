"""Centralized tunables for wisent-compute.

Numeric values here are either:

* MEASURED     – computed from live system state (process RSS, disk usage).
* DERIVED      – calculated from an external constraint or another constant.
* DESIGN       – an explicit operational trade-off (e.g., heartbeat cadence).

Hardcoded guesses about RAM, slot counts, or disk headroom are not allowed.
They must be measured or derived.
"""

# ---------------------------------------------------------------------------
# VRAM
# ---------------------------------------------------------------------------

# DESIGN: hard VRAM safety buffer at admission. Catches jobs whose actual
# peak exceeds their declared gpu_mem_gb. Set to 5% of card VRAM with a 4 GiB
# floor instead of a flat number; on the 96 GiB RTX PRO 6000 this is ~8 GiB.
VRAM_SAFETY_BUFFER_FRACTION = 0.05
VRAM_SAFETY_BUFFER_MIN_GB = 4

# DESIGN: RAM safety buffer. Refuse new slots when MemAvailable drops below
# the measured non-slot RSS plus this buffer. Same 5%-of-total / 4 GiB floor
# rule so it scales across machines instead of being a flat guess.
RAM_SAFETY_BUFFER_FRACTION = 0.05
RAM_SAFETY_BUFFER_MIN_GB = 4

# ---------------------------------------------------------------------------
# Disk
# ---------------------------------------------------------------------------

# DESIGN: stale scratch/output directories older than this are assumed to have
# no active writer and are safe to evict. 1 hour avoids reaping slow checkpoints.
STALE_TRAINING_MAX_AGE_S = 3600

# ---------------------------------------------------------------------------
# Timers / telemetry
# ---------------------------------------------------------------------------

# Main agent poll interval (latency vs. GCS API load trade-off).
POLL_INTERVAL_S = 10

# Capacity broadcast heartbeat. Derived from CAPACITY_STALE_SECONDS so the
# broadcast is always fresh before the stale threshold fires.
CAPACITY_STALE_SECONDS = 180
CAPACITY_HEARTBEAT_INTERVAL_S = max(POLL_INTERVAL_S, CAPACITY_STALE_SECONDS // 3)

# Per-job heartbeat interval. Derived from the 15-minute staleness threshold
# used by the orphan monitor (15 * 60 / 15 = 60 s).
SLOT_HEARTBEAT_INTERVAL_S = 60

# Fleet staging flush interval (~20 commits/hour, well under the HF rate cap).
FLEET_FLUSH_INTERVAL_S = 180

# Minimum runtime before a yieldable slot can be preempted again, so a
# freshly-restarted background job gets real work done before cooperative
# yield thrashes it.
MIN_RUNTIME_BEFORE_YIELD_S = 300

# Cache TTL for the CUDA child-probe in local_agent.py.
CUDA_PROBE_CACHE_S = 30

# ---------------------------------------------------------------------------
# Sizing / capacity caches
# ---------------------------------------------------------------------------

# DESIGN: max completed blobs to sample when building observed_* maps.
COMPLETED_SAMPLE_CAP = 6000

# DESIGN: cache TTL for observed VRAM/RAM maps.
OBSERVED_MAP_TTL_S = 600

# DESIGN: staleness threshold for live capacity broadcasts.
LIVE_CAPACITY_TTL_S = 180
