"""GCP provider: GCE instance lifecycle."""
from __future__ import annotations

import sys
from google.cloud import compute_v1

from ...config import PROJECT, ZONE_ROTATION, MACHINE_TYPE_ZONES, INSTANCE_PREFIX
from ..base import Provider
from .stockout import (
    zone_recently_stocked_out as _zone_recently_stocked_out,
    mark_zone_stockout as _mark_zone_stockout,
    STOCKOUT_TTL_S as _STOCKOUT_TTL_S,
    region_recently_quota_exceeded as _region_quota_exhausted,
    mark_region_quota_exceeded as _mark_region_quota,
    QUOTA_TTL_S as _QUOTA_TTL_S,
)


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
        # Override per-job stored image if a baked agent image family exists.
        # The bake_agent_image.sh script publishes images into family
        # 'wisent-agent' in the local project. When present, every dispatched
        # VM uses the baked image (which already has wisent-compute +
        # transformers + datasets pre-installed) instead of the legacy
        # deeplearning-platform-release base, dropping boot time from
        # ~5-10 install-rotations to ~30 install-secs.
        from google.api_core.exceptions import NotFound as _NotFound
        baked_family = "wisent-agent"
        baked_present = False
        from google.cloud import compute_v1 as _cv1
        images_client = _cv1.ImagesClient()
        try:
            latest = images_client.get_from_family(
                project=self.project, family=baked_family,
            )
        except _NotFound:
            # No baked image family published yet -> use the per-job image
            # argument unchanged. Any other error propagates.
            latest = None
        if latest and latest.name:
            image = latest.name
            image_project = self.project
            baked_present = True
        zones = MACHINE_TYPE_ZONES.get(machine_type, ZONE_ROTATION)
        # Track regions with confirmed QUOTA_EXCEEDED this call. GCP enforces
        # GPU quota at the regional level, so a 403 in one zone means every
        # other zone in the same region will also fail. Without this short-
        # circuit the loop wastes ~10 wall-seconds per zone-retry inside the
        # 60s Cloud Function tick budget — saturated T4 quota in us-central1
        # alone burned 30+ seconds of every tick today, causing 504s.
        skip_regions: set[str] = set()
        for zone in zones:
            region = "-".join(zone.split("-")[:2])
            if region in skip_regions:
                continue
            if _zone_recently_stocked_out(zone):
                _log(f"skip {zone} (recent stockout, TTL {_STOCKOUT_TTL_S}s)")
                continue
            # Cross-call quota cache: previous tick's create_instance found
            # this (region, accel) at QUOTA_EXCEEDED. Skip the API call —
            # quota doesn't change within the 60s TTL window.
            if accel_type and _region_quota_exhausted(region, accel_type):
                _log(f"skip {zone} ({accel_type} quota exhausted in {region}, TTL {_QUOTA_TTL_S}s)")
                continue
            try:
                # Delete any existing terminated instance with same name.
                # NotFound is the desired terminal state; anything else propagates.
                from google.api_core.exceptions import NotFound as _NotFound
                try:
                    self.client.delete(project=self.project, zone=zone, instance=name)
                except _NotFound:
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
                msg = str(e)
                if "already exists" in msg:
                    return f"{name}@{zone}"
                _log(f"Failed in {zone}: {e}")
                if "QUOTA_EXCEEDED" in msg:
                    skip_regions.add(region)
                    if accel_type:
                        _mark_region_quota(region, accel_type)
                if "ZONE_RESOURCE_POOL_EXHAUSTED" in msg or "STOCKOUT" in msg:
                    _mark_zone_stockout(zone)
                continue
        return None

    def delete_instance(self, instance_ref: str):
        from google.api_core.exceptions import NotFound
        name, zone = instance_ref.split("@")
        try:
            self.client.delete(project=self.project, zone=zone, instance=name)
        except NotFound:
            # Idempotent: already-deleted instance is the desired terminal
            # state. Any other API error propagates so the caller sees it.
            return

    def instance_exists(self, instance_ref: str) -> bool:
        from google.api_core.exceptions import NotFound
        name, zone = instance_ref.split("@")
        try:
            inst = self.client.get(project=self.project, zone=zone, instance=name)
        except NotFound:
            return False
        return inst.status in ("RUNNING", "STAGING", "PROVISIONING")

    def instance_lifecycle_state(self, instance_ref: str) -> str | None:
        """Return raw GCE status: RUNNING/TERMINATED/STOPPED/STAGING/etc."""
        from google.api_core.exceptions import NotFound
        name, zone = instance_ref.split("@")
        try:
            inst = self.client.get(project=self.project, zone=zone, instance=name)
        except NotFound:
            return None
        return inst.status

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
        return [r for r, _ in self.list_running_instance_refs_with_age()]

    def list_running_instance_refs_with_age(self) -> list[tuple[str, float]]:
        """[(name@zone, age_in_seconds), ...] for all RUNNING wisent-agent VMs.
        age_in_seconds is now - creationTimestamp. Used by the never-worked
        reaper branch to apply a grace period from VM boot before culling."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        out = []
        request = compute_v1.AggregatedListInstancesRequest(
            project=self.project,
            filter=f"name:{INSTANCE_PREFIX}-agent-*",
        )
        for zone_path, response in self.client.aggregated_list(request=request):
            zone = zone_path.split("/")[-1]
            for inst in response.instances or []:
                if inst.status != "RUNNING":
                    continue
                created = getattr(inst, "creation_timestamp", None) or ""
                age = 0.0
                if created:
                    ct = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    age = (now - ct).total_seconds()
                out.append((f"{inst.name}@{zone}", age))
        return out
