"""Common data structure for provisioning"""
from typing import List, Dict, Optional
import dataclasses


@dataclasses.dataclass
class ProvisionMetadata:
    """Metadata from provisioning."""
    region: str
    zone: str
    head_instance_id: str
    resumed_instance_ids: List[str]
    created_instance_ids: List[str]


@dataclasses.dataclass
class InstanceMetadata:
    """Metadata from querying a cloud instance."""
    instance_id: str
    private_ip: Optional[str]
    public_ip: Optional[str]
    tags: Dict[str, str]


@dataclasses.dataclass
class ClusterMetadata:
    """Metadata from querying a cluster."""
    instances: Dict[str, InstanceMetadata]
    head_instance_id: Optional[str]

    def ip_tuples(self) -> List:
        """Get IP tuples of all instances. Make sure that list always
        starts with head node IP, if head node exists.
        """
        head_node_ip, other_ips = [], []
        for inst in self.instances.values():
            pair = (inst.private_ip, inst.public_ip)
            if inst.instance_id == self.head_instance_id:
                head_node_ip.append(pair)
            else:
                other_ips.append(pair)
        return head_node_ip + other_ips
