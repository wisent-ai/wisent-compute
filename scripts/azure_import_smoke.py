"""Import-smoke for the Azure additions to wisent-compute.

Loads each modified module through the package import path so a bad
relative import or missing symbol surfaces here, without needing the
azure-* SDKs (those are imported lazily inside the providers/backends).
"""
import wisent_compute.config as cfg
print("config OK; WC_PROVIDERS=", cfg.WC_PROVIDERS,
      "WC_STORAGE_BACKEND=", cfg.WC_STORAGE_BACKEND,
      "AZURE_LOCATIONS=", cfg.AZURE_LOCATIONS[:2])

import wisent_compute.models as m
print("models OK; azure tier 80 ->", m.GPU_SIZING["azure"][80],
      "azure rate NC4 ->", m.AZURE_VM_HOURLY_RATE_USD["Standard_NC4as_T4_v3"])

from wisent_compute.providers import get_provider
print("providers OK; gcp ->", type(get_provider("gcp")).__name__)
try:
    get_provider("azure")
    print("azure provider constructed (subscription set)")
except RuntimeError as e:
    print("azure provider constructor raised (expected without creds):", repr(e)[:120])
except ImportError as e:
    print("azure SDK not installed (optional extra):", repr(e)[:120])
except Exception as e:
    print("azure provider raised:", type(e).__name__, repr(e)[:120])

import wisent_compute.scheduler.quota as q
print("quota OK; load_quotas defaults ->", q.load_quotas.__defaults__)

import wisent_compute.scheduler.cost as cst
print("cost OK; Azure on-demand for Standard_NC4as_T4_v3:",
      cst._hourly_rate_usd("nvidia-tesla-t4", False, "Standard_NC4as_T4_v3"),
      "spot:",
      cst._hourly_rate_usd("nvidia-tesla-t4", True, "Standard_NC4as_T4_v3"))
print("cost OK; GCP on-demand for nvidia-tesla-a100 + a2-highgpu-1g:",
      cst._hourly_rate_usd("nvidia-tesla-a100", False, "a2-highgpu-1g"))

import wisent_compute.scheduler.dispatch.agent as da
print("dispatch.agent OK; templates ->", da._TEMPLATES_BY_PROVIDER)

import wisent_compute.queue.storage as st
print("storage OK; backend_name property exists ->",
      hasattr(st.JobStorage, "backend_name"))

import wisent_compute.queue.capacity as cap
print("capacity OK; CAPACITY_PREFIX=", cap.CAPACITY_PREFIX)

import wisent_compute.coordinator as co
print("coordinator OK")

import wisent_compute.cloud_function.main as cf
print("cloud_function.main OK")

import wisent_compute.cli as cli
print("cli OK")

print("ALL IMPORTS PASS")

# Render the submit + agent help so we see the new --provider/--kind copy.
from click.testing import CliRunner
runner = CliRunner()
res = runner.invoke(cli.main, ["submit", "--help"])
print("--- wc submit --help ---")
print(res.output)
res = runner.invoke(cli.main, ["agent", "--help"])
print("--- wc agent --help ---")
print(res.output)
