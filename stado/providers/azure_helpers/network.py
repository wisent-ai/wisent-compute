"""Azure network helpers — subnet/NSG URI construction + NIC lifecycle.

Naming convention: vnet `${AZURE_VNET}-{location}`, NSG `${AZURE_NSG}-
{location}`, subnet `${AZURE_SUBNET}` (subnet is a child of vnet so its
name is locally scoped to the already-region-specific vnet).
"""
from __future__ import annotations

import sys


def _log(msg: str) -> None:
    sys.stderr.write(f"[azure-net] {msg}\n")
    sys.stderr.flush()


def subnet_id(subscription: str, rg: str, vnet: str, subnet: str,
              location: str) -> str:
    return (
        f"/subscriptions/{subscription}"
        f"/resourceGroups/{rg}"
        f"/providers/Microsoft.Network/virtualNetworks/{vnet}-{location}"
        f"/subnets/{subnet}"
    )


def nsg_id(subscription: str, rg: str, nsg: str, location: str) -> str:
    if not nsg:
        return ""
    return (
        f"/subscriptions/{subscription}"
        f"/resourceGroups/{rg}"
        f"/providers/Microsoft.Network/networkSecurityGroups/{nsg}-{location}"
    )


def create_nic(network_client, rg: str, name: str, location: str,
               subnet_id_str: str, nsg_id_str: str) -> str:
    body: dict = {
        "location": location,
        "ip_configurations": [{
            "name": "ipcfg",
            "subnet": {"id": subnet_id_str},
        }],
    }
    if nsg_id_str:
        body["network_security_group"] = {"id": nsg_id_str}
    op = network_client.network_interfaces.begin_create_or_update(
        rg, f"{name}-nic", body,
    )
    return op.result().id


def delete_nic(network_client, rg: str, name: str) -> None:
    try:
        network_client.network_interfaces.begin_delete(rg, f"{name}-nic")
    except Exception as e:
        _log(f"NIC delete failed for {name}-nic: {e!r}")
