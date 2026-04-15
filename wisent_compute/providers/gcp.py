"""GCP provider: GCE instance lifecycle."""
from __future__ import annotations

import sys
from google.cloud import compute_v1

from ..config import PROJECT, ZONE_ROTATION, INSTANCE_PREFIX
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
                        startup_script) -> str | None:
        for zone in ZONE_ROTATION:
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
                sched = compute_v1.Scheduling(
                    preemptible=False, on_host_maintenance="TERMINATE",
                )
                accels = []
                if accel_type:
                    accels.append(compute_v1.AcceleratorConfig(
                        accelerator_type=f"zones/{zone}/acceleratorTypes/{accel_type}",
                        accelerator_count=1,
                    ))
                inst = compute_v1.Instance(
                    name=name,
                    machine_type=f"zones/{zone}/machineTypes/{machine_type}",
                    disks=[disk], network_interfaces=[net],
                    metadata=meta, scheduling=sched,
                    guest_accelerators=accels,
                )
                op = self.client.insert(project=self.project, zone=zone, instance_resource=inst)
                op.result()
                _log(f"Created {name}@{zone}")
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
