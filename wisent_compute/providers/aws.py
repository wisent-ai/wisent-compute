"""AWS provider: EC2 instance lifecycle."""
from __future__ import annotations

import os
import sys
import base64

import boto3

from .base import Provider

REGION = os.environ.get("AWS_REGION", "us-east-1")
AZ_ORDER = [f"{REGION}a", f"{REGION}c", f"{REGION}d", f"{REGION}b"]


def _log(msg):
    sys.stderr.write(f"[aws] {msg}\n")
    sys.stderr.flush()


class AWSProvider(Provider):
    def __init__(self):
        self.ec2 = boto3.client("ec2", region_name=REGION)

    def create_instance(self, name, machine_type, accel_type,
                        boot_disk_gb, image, image_project,
                        startup_script, preemptible: bool = False) -> str | None:
        # AWS Spot instances would require RequestSpotInstances + a different
        # lifecycle than RunInstances. The current implementation always boots
        # on-demand; preemptible=True is accepted for interface compatibility
        # but is not yet wired through.
        sg = os.environ.get("AWS_SECURITY_GROUP", "")
        iam = os.environ.get("AWS_IAM_PROFILE", "wisent-instance-profile")
        ud = base64.b64encode(startup_script.encode()).decode()

        for az in AZ_ORDER:
            try:
                subnets = self.ec2.describe_subnets(
                    Filters=[{"Name": "availability-zone", "Values": [az]}])
                if not subnets["Subnets"]:
                    continue
                subnet = subnets["Subnets"][0]["SubnetId"]
                resp = self.ec2.run_instances(
                    ImageId=image, InstanceType=machine_type,
                    SecurityGroupIds=[sg], SubnetId=subnet,
                    IamInstanceProfile={"Name": iam},
                    UserData=ud,
                    BlockDeviceMappings=[{
                        "DeviceName": "/dev/sda1",
                        "Ebs": {"VolumeSize": boot_disk_gb, "VolumeType": "gp3",
                                "DeleteOnTermination": True},
                    }],
                    TagSpecifications=[{
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": name}],
                    }],
                    MinCount=1, MaxCount=1,
                )
                iid = resp["Instances"][0]["InstanceId"]
                _log(f"Created {iid} in {az}")
                return iid
            except Exception as e:
                if "InsufficientInstanceCapacity" in str(e):
                    continue
                _log(f"Failed in {az}: {e}")
                continue
        return None

    def delete_instance(self, instance_ref: str):
        from botocore.exceptions import ClientError
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_ref])
        except ClientError as exc:
            # InvalidInstanceID.NotFound is the desired terminal state.
            # Anything else propagates.
            if exc.response.get("Error", {}).get("Code") != "InvalidInstanceID.NotFound":
                raise

    def instance_exists(self, instance_ref: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            r = self.ec2.describe_instances(InstanceIds=[instance_ref])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "InvalidInstanceID.NotFound":
                return False
            raise
        state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
        return state in ("running", "pending")

    def list_running_instances(self) -> dict[str, int]:
        counts = {}
        paginator = self.ec2.get_paginator("describe_instances")
        for page in paginator.paginate(Filters=[
            {"Name": "tag:Name", "Values": ["wisent-*"]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]):
            for res in page["Reservations"]:
                for inst in res["Instances"]:
                    itype = inst["InstanceType"]
                    counts[itype] = counts.get(itype, 0) + 1
        return counts
