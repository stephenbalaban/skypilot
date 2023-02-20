"""All provisioners supported by Skypilot."""

import typing
from typing import Dict, Any

import abc
import importlib
import types

from sky.provision import common


class ProviderModule(types.ModuleType, metaclass=abc.ABCMeta):
    """This abstract imitates a provider module."""

    @staticmethod
    def bootstrap(config: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @staticmethod
    def start_instances(region: str, cluster_name: str, node_config: Dict[str,
                                                                          Any],
                        tags: Dict[str, str], count: int,
                        resume_stopped_nodes: bool) -> common.ProvisionMetadata:
        pass

    def stop_instances(self, region: str, cluster_name: str) -> None:
        pass

    def terminate_instances(self, region: str, cluster_name: str) -> None:
        pass

    def stop_instances_with_self(self) -> None:
        pass

    def terminate_instances_with_self(self) -> None:
        pass

    def wait_instances(self, region: str, cluster_name: str,
                       state: str) -> None:
        pass

    def get_cluster_metadata(self, region: str,
                             cluster_name: str) -> common.ClusterMetadata:
        pass


def get(provisioner_name: str) -> ProviderModule:
    module = importlib.import_module(
        f'sky.provision.{provisioner_name.lower()}')
    return typing.cast(ProviderModule, module)
