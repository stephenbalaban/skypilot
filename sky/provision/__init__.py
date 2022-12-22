"""All provisioners supported by Skypilot."""

import typing
from typing import Dict, Any, Optional

import abc
import importlib
import types


class ProviderModule(types.ModuleType, metaclass=abc.ABCMeta):
    """This abstract imitates a provider module."""

    @staticmethod
    def bootstrap(config: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @staticmethod
    def create_or_resume_instances(
            region: str, cluster_name: str, node_config: Dict[str, Any],
            tags: Dict[str, str], count: int,
            resume_stopped_nodes: bool) -> Dict[str, Any]:
        pass

    def resume_instances(self,
                         region: str,
                         cluster_name: str,
                         tags: Dict[str, str],
                         count: Optional[int] = None) -> Dict[str, Any]:
        pass

    def stop_instances(self, region: str, cluster_name: str):
        pass

    def terminate_instances(self, region: str, cluster_name: str):
        pass

    def stop_instances_with_self(self):
        pass

    def terminate_instances_with_self(self):
        pass

    def wait_instances(self, region: str, cluster_name: str, state: str):
        pass

    def get_instance_ips(self, region: str, cluster_name: str):
        pass


def get(provisioner_name: str) -> ProviderModule:
    module = importlib.import_module(
        f'sky.provision.{provisioner_name.lower()}')
    return typing.cast(ProviderModule, module)
