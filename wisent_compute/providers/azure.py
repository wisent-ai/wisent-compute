"""Azure provider: Azure VM lifecycle.

Mirrors providers/gcp.py. Each `wc submit --provider azure ...` lands a job
whose dispatcher path goes through this module: NIC + VM create across the
configured AZURE_LOCATIONS list, fall through quota/capacity errors per
region until one accepts the call. Pre-provisioned vnet/subnet/NSG named in
config are attached to the NIC; the provider does NOT create network
infrastructure on demand.

Naming: Azure resource names are kebab-case-friendly; the same `wisent-...`
prefix used on GCE works as-is. instance_ref is "name@location" so the
existing scheduler/monitor split-on-@ logic stays consistent across
providers.
"""
from __future__ import annotations

import base64
import os
import sys
from datetime import datetime, timezone

from ..config import (
    AZURE_IMAGE_URN,
    AZURE_LOCATIONS,
    AZURE_NSG,
    AZURE_RESOURCE_GROUP,
    AZURE_SSH_PUBLIC_KEY,
    AZURE_SUBNET,
    AZURE_SUBSCRIPTION_ID,
    AZURE_VM_USERNAME,
    AZURE_VNET,
    INSTANCE_PREFIX,
)
from ..models import AZURE_VM_TO_ACCEL
from .base import Provider


def _log(msg):
    sys.stderr.write(f"[azure] {msg}\n")
    sys.stderr.flush()


def _parse_image_urn(urn: str) -> dict:
    """Convert publisher:offer:sku:version into the dict shape Azure expects."""
    parts = urn.split(":")
    if len(parts) != 4:
        raise ValueError(
            f"AZURE_IMAGE_URN must be 'publisher:offer:sku:version', got {urn!r}"
        )
    publisher, offer, sku, version = parts
    return {
        "publisher": publisher,
        "offer": offer,
        "sku": sku,
        "version": version,
    }


class AzureProvider(Provider):
    def __init__(self):
        # Lazy-import azure SDKs so non-azure installs (no `[azure]` extra)
        # never pay the import cost; constructor will raise if creds missing.
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.compute import ComputeManagementClient
        from azure.mgmt.network import NetworkManagementClient

        if not AZURE_SUBSCRIPTION_ID:
            raise RuntimeError(
                "AZURE_SUBSCRIPTION_ID env var is empty; cannot construct AzureProvider"
            )
        cred = DefaultAzureCredential()
        self.compute = ComputeManagementClient(cred, AZURE_SUBSCRIPTION_ID)
        self.network = NetworkManagementClient(cred, AZURE_SUBSCRIPTION_ID)
        self.subscription = AZURE_SUBSCRIPTION_ID
        self.rg = AZURE_RESOURCE_GROUP
        self.locations = list(AZURE_LOCATIONS)
        self.vnet = AZURE_VNET
        self.subnet = AZURE_SUBNET
        self.nsg = AZURE_NSG
        self.image_urn = AZURE_IMAGE_URN
        self.username = AZURE_VM_USERNAME
        self.ssh_public_key = AZURE_SSH_PUBLIC_KEY

    # ----- helpers --------------------------------------------------------

    def _subnet_id(self, location: str) -> str:
        """Build the absolute resource ID for the configured subnet in this location.

        Azure SDK lets us pass either the full ID string or a ref-by-name.
        Full ID avoids one network.subnets.get round trip per create_instance
        call; the URI shape is documented and stable across regions.
        """
        return (
            f"/subscriptions/{self.subscription}"
            f"/resourceGroups/{self.rg}"
            f"/providers/Microsoft.Network/virtualNetworks/{self.vnet}"
            f"/subnets/{self.subnet}"
        )

    def _nsg_id(self) -> str:
        if not self.nsg:
            return ""
        return (
            f"/subscriptions/{self.subscription}"
            f"/resourceGroups/{self.rg}"
            f"/providers/Microsoft.Network/networkSecurityGroups/{self.nsg}"
        )

    def _create_nic(self, name: str, location: str) -> str:
        """Create the NIC bound to the pre-provisioned subnet/NSG. Returns NIC ID."""
        body: dict = {
            "location": location,
            "ip_configurations": [{
                "name": "ipcfg",
                "subnet": {"id": self._subnet_id(location)},
            }],
        }
        nsg_id = self._nsg_id()
        if nsg_id:
            body["network_security_group"] = {"id": nsg_id}
        op = self.network.network_interfaces.begin_create_or_update(
            self.rg, f"{name}-nic", body,
        )
        nic = op.result()
        return nic.id

    def _delete_nic(self, name: str):
        """Best-effort NIC cleanup. Failures are logged but don't block delete_instance."""
        try:
            self.network.network_interfaces.begin_delete(self.rg, f"{name}-nic")
        except Exception as e:
            _log(f"NIC delete failed for {name}-nic: {e!r}")

    # ----- Provider interface --------------------------------------------

    def create_instance(self, name, machine_type, accel_type,
                        boot_disk_gb, image, image_project,
                        startup_script, preemptible: bool = False) -> str | None:
        # Per-location skip set after a quota refusal; same family is shared
        # across the whole region so retrying other zones-in-region wouldn't help.
        skipped: set[str] = set()
        for location in self.locations:
            if location in skipped:
                continue
            try:
                nic_id = self._create_nic(name, location)
            except Exception as e:
                msg = str(e)
                _log(f"NIC create failed in {location}: {e}")
                if "QuotaExceeded" in msg or "OperationNotAllowed" in msg:
                    skipped.add(location)
                continue

            try:
                vm_body: dict = {
                    "location": location,
                    "tags": {
                        "wisent_managed": "true",
                        "wisent_created": datetime.now(timezone.utc).isoformat(),
                    },
                    "hardware_profile": {"vm_size": machine_type},
                    "storage_profile": {
                        "image_reference": _parse_image_urn(self.image_urn),
                        "os_disk": {
                            "create_option": "FromImage",
                            "disk_size_gb": int(boot_disk_gb),
                            "managed_disk": {"storage_account_type": "Premium_LRS"},
                            "delete_option": "Delete",
                        },
                    },
                    "os_profile": {
                        "computer_name": name[:15],  # Azure caps Linux hostname at 15
                        "admin_username": self.username,
                        "custom_data": base64.b64encode(
                            startup_script.encode()
                        ).decode(),
                        "linux_configuration": {
                            "disable_password_authentication": True,
                            "ssh": {
                                "public_keys": [{
                                    "path": f"/home/{self.username}/.ssh/authorized_keys",
                                    "key_data": self.ssh_public_key,
                                }],
                            } if self.ssh_public_key else {},
                        },
                    },
                    "network_profile": {
                        "network_interfaces": [{
                            "id": nic_id,
                            "properties": {"primary": True, "delete_option": "Delete"},
                        }],
                    },
                }
                if preemptible:
                    # Azure Spot: priority="Spot", eviction_policy="Delete" so a
                    # preempted VM is fully removed (matches GCP's
                    # instance_termination_action="DELETE"). billing_profile
                    # max_price=-1 means "pay up to on-demand list price",
                    # i.e. take whatever Spot capacity is available without
                    # an explicit cap. The scheduler enforces cost via
                    # max_cost_per_hour_usd separately.
                    vm_body["priority"] = "Spot"
                    vm_body["eviction_policy"] = "Delete"
                    vm_body["billing_profile"] = {"max_price": -1.0}

                op = self.compute.virtual_machines.begin_create_or_update(
                    self.rg, name, vm_body,
                )
                op.result()
                _log(f"Created {name}@{location} preemptible={preemptible}")
                return f"{name}@{location}"
            except Exception as e:
                msg = str(e)
                if "already exists" in msg.lower():
                    return f"{name}@{location}"
                _log(f"VM create failed in {location}: {e}")
                # Roll back the NIC we just created so we don't leak it.
                self._delete_nic(name)
                if "QuotaExceeded" in msg or "OperationNotAllowed" in msg or "SkuNotAvailable" in msg:
                    skipped.add(location)
                continue
        return None

    def delete_instance(self, instance_ref: str):
        try:
            name, _ = instance_ref.split("@")
        except ValueError:
            _log(f"delete_instance: malformed ref {instance_ref!r}")
            return
        try:
            self.compute.virtual_machines.begin_delete(self.rg, name)
        except Exception as e:
            _log(f"VM delete failed for {name}: {e!r}")
        # NIC cleanup is best-effort; a leaked NIC in the resource group is
        # surfaced by the never-worked reaper's age-based scan on the next
        # tick anyway.
        self._delete_nic(name)

    def instance_exists(self, instance_ref: str) -> bool:
        try:
            name, _ = instance_ref.split("@")
        except ValueError:
            return False
        try:
            vm = self.compute.virtual_machines.get(
                self.rg, name, expand="instanceView",
            )
        except Exception:
            return False
        # provisioningState == "Succeeded" + power_state in (running, starting)
        # is the closest analogue to GCE RUNNING/STAGING/PROVISIONING. Azure
        # also has "Updating", which we treat as alive — a VM mid-update is
        # still consuming GPU quota and shouldn't be requeued.
        prov = (vm.provisioning_state or "").lower()
        if prov in ("creating", "updating", "succeeded"):
            statuses = []
            iv = getattr(vm, "instance_view", None)
            if iv is not None:
                statuses = [s.code for s in (iv.statuses or [])]
            for code in statuses:
                if code.startswith("PowerState/"):
                    state = code.split("/", 1)[1]
                    return state in ("running", "starting")
            # Mid-create: no PowerState yet — treat as alive.
            return prov in ("creating", "updating")
        return False

    def instance_lifecycle_state(self, instance_ref: str) -> str | None:
        """Return the literal Azure power-state ('running', 'deallocated', ...).

        The monitor uses lifecycle_state == "TERMINATED" (GCE) to detect Spot
        preemption. On Azure, Spot eviction lands the VM in PowerState/deallocated
        — the monitor treats that string as the preemption signal.
        """
        try:
            name, _ = instance_ref.split("@")
        except ValueError:
            return None
        try:
            vm = self.compute.virtual_machines.get(
                self.rg, name, expand="instanceView",
            )
        except Exception:
            return None
        iv = getattr(vm, "instance_view", None)
        if iv is None:
            return None
        for s in iv.statuses or []:
            if (s.code or "").startswith("PowerState/"):
                return s.code.split("/", 1)[1]  # running/deallocated/stopped/...
        return None

    def list_running_instances(self) -> dict[str, int]:
        """{accel_type: count} for all live wisent-* VMs across the resource group."""
        counts: dict[str, int] = {}
        try:
            vms = list(self.compute.virtual_machines.list(self.rg))
        except Exception as e:
            _log(f"list_running_instances failed: {e!r}")
            return {}
        for vm in vms:
            if not (vm.name or "").startswith(f"{INSTANCE_PREFIX}-"):
                continue
            # Cheap state probe: list() doesn't include instance_view by
            # default, so trust the tag we stamped at create-time. A VM in
            # the resource group with the wisent_managed tag and a known
            # GPU SKU consumes quota until we delete it; counting it as
            # running is the safe direction.
            sku = ""
            try:
                sku = vm.hardware_profile.vm_size or ""
            except Exception:
                sku = ""
            spec = AZURE_VM_TO_ACCEL.get(sku)
            if not spec:
                continue
            accel, n = spec
            counts[accel] = counts.get(accel, 0) + int(n)
        return counts

    def list_running_instance_refs(self) -> list[str]:
        return [r for r, _ in self.list_running_instance_refs_with_age()]

    def list_running_instance_refs_with_age(self) -> list[tuple[str, float]]:
        """[(name@location, age_in_seconds), ...] for live wisent-agent-* VMs.

        Mirrors providers/gcp.py — restricts to '<prefix>-agent-*' so the
        dead-agent reaper doesn't sweep unrelated wisent-* VMs.
        """
        out: list[tuple[str, float]] = []
        now = datetime.now(timezone.utc)
        try:
            vms = list(self.compute.virtual_machines.list(self.rg))
        except Exception as e:
            _log(f"list_running_instance_refs_with_age failed: {e!r}")
            return []
        for vm in vms:
            name = vm.name or ""
            if not name.startswith(f"{INSTANCE_PREFIX}-agent-"):
                continue
            tags = dict(vm.tags or {})
            created = tags.get("wisent_created", "")
            age = 0.0
            if created:
                try:
                    ct = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    age = (now - ct).total_seconds()
                except Exception:
                    age = 0.0
            out.append((f"{name}@{vm.location}", age))
        return out
