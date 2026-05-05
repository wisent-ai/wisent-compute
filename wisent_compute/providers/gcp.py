"""GCP provider: GCE instance lifecycle."""
from __future__ import annotations

import sys
from google.cloud import compute_v1

from ..config import PROJECT, ZONE_ROTATION, MACHINE_TYPE_ZONES, INSTANCE_PREFIX
from .base import Provider


def _log(msg):
    sys.stderr.write(f"[gcp] {msg}\n")
    sys.stderr.flush()


class GCPProvider(Provider):
    def __init__(self):
        self.client = compute_v1.InstancesClient()
        self.project = PROJECT

    def create_instance(self, name, machine_type, accel_type,
                        boot_disk_gb, image, image_project,
                        startup_script, preemptible: bool = False) -> str | None:
        zones = MACHINE_TYPE_ZONES.get(machine_type, ZONE_ROTATION)
        for zone in zones:
            try:
                # Delete any existing terminated instance with same name
                try:
                    self.client.delete(project=self.project, zone=zone, instance=name)
                except Exception:
                    pass

                disk = compute_v1.AttachedDisk(
                    auto_delete=True, boot=True,
                    initialize_params=compute_v1.AttachedDiskInitializeParams(
                        disk_size_gb=boot_disk_gb,
                        source_image=f"projects/{image_project}/global/images/{image}",
                    ),
                )
                net = compute_v1.NetworkInterface(
                    access_configs=[compute_v1.AccessConfig(name="External NAT")],
                )
                meta = compute_v1.Metadata(items=[
                    compute_v1.Items(key="startup-script", value=startup_script),
                ])
                if preemptible:
                    # Use Spot (the modern provisioning model). The legacy
                    # `preemptible` flag is a separate Bool that GCP keeps for
                    # back-compat; setting both is redundant but explicit.
                    # instance_termination_action="DELETE" so a preempted VM is
                    # fully removed (disk + instance), not just STOPped. With
                    # STOP, every preemption left a zombie TERMINATED instance
                    # holding 200GB of regional disk quota — empirically we
                    # accumulated 546 of them in 4 days, eating ~109TB and
                    # bottlenecking dispatch with DISKS_TOTAL_GB QUOTA_EXCEEDED.
                    sched = compute_v1.Scheduling(
                        preemptible=True,
                        provisioning_model="SPOT",
                        on_host_maintenance="TERMINATE",
                        instance_termination_action="DELETE",
                    )
                else:
                    sched = compute_v1.Scheduling(
                        preemptible=False, on_host_maintenance="TERMINATE",
                    )
                accels = []
                if accel_type:
                    accels.append(compute_v1.AcceleratorConfig(
                        accelerator_type=f"zones/{zone}/acceleratorTypes/{accel_type}",
                        accelerator_count=1,
                    ))
                # Attach wisent-compute-sa so the instance can write status +
                # heartbeat to GCS, pull HF models with the in-startup token,
                # and fetch from gcloud APIs. Without an SA attached, the
                # metadata service returns 404 for default tokens and the
                # whole startup script crashes before extraction begins.
                sa = compute_v1.ServiceAccount(
                    email=f"wisent-compute-sa@{self.project}.iam.gserviceaccount.com",
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                inst = compute_v1.Instance(
                    name=name,
                    machine_type=f"zones/{zone}/machineTypes/{machine_type}",
                    disks=[disk], network_interfaces=[net],
                    metadata=meta, scheduling=sched,
                    guest_accelerators=accels,
                    service_accounts=[sa],
                )
                op = self.client.insert(project=self.project, zone=zone, instance_resource=inst)
                op.result()
                _log(f"Created {name}@{zone} preemptible={preemptible}")
                return f"{name}@{zone}"
            except Exception as e:
                if "already exists" in str(e):
                    return f"{name}@{zone}"
                _log(f"Failed in {zone}: {e}")
                continue
        return None

    def delete_instance(self, instance_ref: str):
        name, zone = instance_ref.split("@")
        try:
            self.client.delete(project=self.project, zone=zone, instance=name)
        except Exception:
            pass

    def instance_exists(self, instance_ref: str) -> bool:
        name, zone = instance_ref.split("@")
        try:
            inst = self.client.get(project=self.project, zone=zone, instance=name)
            return inst.status in ("RUNNING", "STAGING", "PROVISIONING")
        except Exception:
            return False

    def instance_lifecycle_state(self, instance_ref: str) -> str | None:
        """Return raw GCE status: RUNNING/TERMINATED/STOPPED/STAGING/etc."""
        name, zone = instance_ref.split("@")
        try:
            inst = self.client.get(project=self.project, zone=zone, instance=name)
            return inst.status
        except Exception:
            return None

    def list_running_instances(self) -> dict[str, int]:
        counts = {}
        request = compute_v1.AggregatedListInstancesRequest(
            project=self.project,
            filter=f"name:{INSTANCE_PREFIX}-*",
        )
        for zone, response in self.client.aggregated_list(request=request):
            for inst in response.instances or []:
                if inst.status not in ("RUNNING", "STAGING", "PROVISIONING"):
                    continue
                for acc in inst.guest_accelerators or []:
                    atype = acc.accelerator_type.split("/")[-1]
                    counts[atype] = counts.get(atype, 0) + acc.accelerator_count
        return counts

    def list_running_instance_refs(self) -> list[str]:
        """[(name@zone), ...] for all RUNNING wisent-agent VMs. Used by the
        dead-agent reaper to cross-reference against live capacity broadcasts.

        The filter intentionally narrows to `<prefix>-agent-*` (not just
        `<prefix>-*`): the broader pattern also matches unrelated service
        MIG instances in the same project (`wisent-mig-api-*`,
        `wisent-mig-inference-*`, `wisent-mig-images-*`) which never
        broadcast capacity, so the reaper would mass-delete them every tick.
        """
        refs = []
        request = compute_v1.AggregatedListInstancesRequest(
            project=self.project,
            filter=f"name:{INSTANCE_PREFIX}-agent-*",
        )
        for zone_path, response in self.client.aggregated_list(request=request):
            zone = zone_path.split("/")[-1]
            for inst in response.instances or []:
                if inst.status == "RUNNING":
                    refs.append(f"{inst.name}@{zone}")
        return refs
