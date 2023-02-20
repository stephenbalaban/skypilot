"""A cloud-neutral VM provision backend."""
from typing import List, Optional, Dict, Any
import logging
import os
import pathlib
import time
import traceback

import colorama

from sky import clouds
from sky import provision
from sky import sky_logging

from sky.backends import backend_utils
from sky.provision import common as provision_comm
from sky.provision import setup as provision_setup
from sky.provision import utils as provision_utils
from sky.utils import command_runner
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

_MAX_RETRY = 3


def _bulk_provision(
    cloud: clouds.Cloud,
    region: str,
    zones: List[clouds.Zone],
    cluster_name: str,
    num_nodes: int,
    initial_config: Dict[str, Any],
) -> provision_comm.ProvisionMetadata:
    """Provisions a cluster and wait until fully provisioned."""
    provider = provision.get(repr(cloud))

    style = colorama.Style

    if not zones:
        # For Azure, zones is always an empty list.
        zone_str = 'all zones'
    else:
        zone_str = ','.join(z.name for z in zones)

    if isinstance(cloud, clouds.Local):
        logger.info(f'{style.BRIGHT}Launching on local cluster '
                    f'{cluster_name!r}.')
    else:
        logger.info(f'{style.BRIGHT}Launching on {cloud} '
                    f'{region}{style.RESET_ALL} ({zone_str})')
    start = time.time()

    # 5 seconds to 180 seconds. We need backoff for e.g., rate limit per
    # minute errors.
    backoff = common_utils.Backoff(initial_backoff=5,
                                   max_backoff_factor=180 // 5)

    # TODO(suquark): Should we just check the cluster status
    #  if 'cluster_exists' is true? Then we can skip bootstrapping
    #  etc if all nodes are ready. This is a known issue before.

    try:
        with backend_utils.safe_console_status(
                f'[bold cyan]Bootstrapping configurations for '
                f'[green]{cluster_name}[white] ...'):
            config = provider.bootstrap(initial_config)
    except Exception:
        logger.error('Failed to bootstrap configurations for '
                     f'"{cluster_name}".')
        raise

    for retry_cnt in range(_MAX_RETRY):
        try:
            with backend_utils.safe_console_status(
                    f'[bold cyan]Starting instances for '
                    f'[green]{cluster_name}[white] ...'):
                provision_metadata = provider.start_instances(
                    region,
                    cluster_name,
                    config['node_config'], {},
                    count=num_nodes,
                    resume_stopped_nodes=True)
            break
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Starting instances for "{cluster_name}" '
                         f'failed. Stacktrace:\n{traceback.format_exc()}')
            if retry_cnt >= _MAX_RETRY - 1:
                logger.error(f'Failed to provision "{cluster_name}" after '
                             'maximum retries.')
                raise e
            sleep = backoff.current_backoff()
            logger.info('Retrying launching in {:.1f} seconds.'.format(sleep))
            time.sleep(sleep)

    logger.debug(f'Waiting "{cluster_name}" to be started...')
    with backend_utils.safe_console_status(
            f'[bold cyan]Waiting '
            f'[green]{cluster_name}[bold cyan] to be started...'):
        provider.wait_instances(region, cluster_name, 'running')

    logger.debug(f'Cluster up takes {time.time() - start} seconds with '
                 f'{retry_cnt} retries.')

    plural = '' if num_nodes == 1 else 's'
    if not isinstance(cloud, clouds.Local):
        logger.info(f'{colorama.Fore.GREEN}Successfully provisioned '
                    f'or found existing VM{plural}.{style.RESET_ALL}')
    return provision_metadata


def bulk_provision(
    cloud: clouds.Cloud,
    region: clouds.Region,
    zones: List[clouds.Zone],
    handle: 'CloudVmRayBackend.ResourceHandle',
    is_prev_cluster_healthy: bool,
    log_dir: str,
) -> Optional[provision_comm.ProvisionMetadata]:
    log_dir = os.path.abspath(os.path.expanduser(log_dir))
    os.makedirs(log_dir, exist_ok=True)
    log_abs_path = os.path.join(log_dir, 'provision.log')

    cluster_name = handle.cluster_name
    num_nodes = handle.launched_nodes
    original_config = common_utils.read_yaml(handle.cluster_yaml)
    init_config = {
        'cluster_name': cluster_name,
        'provider': original_config['provider'],
        'auth': original_config['auth'],
        'node_config': original_config['available_node_types']
                       ['ray.head.default']['node_config'],
    }
    init_config['provider']['num_nodes'] = num_nodes

    fh = logging.FileHandler(log_abs_path)
    fh.setLevel(logging.DEBUG)
    try:
        logger.addHandler(fh)
        return _bulk_provision(cloud, region.name, zones, cluster_name,
                               num_nodes, init_config)
    except Exception:  # pylint: disable=broad-except
        logger.exception(f'Provision cluster {cluster_name} failed.')
        logger.error('*** Failed provisioning the cluster. ***')

        # If cluster was previously UP or STOPPED, stop it; otherwise
        # terminate.
        # FIXME(zongheng): terminating a potentially live cluster is
        # scary. Say: users have an existing cluster that got into INIT, do
        # sky launch, somehow failed, then we may be terminating it here.
        terminate = not is_prev_cluster_healthy
        terminate_str = ('Terminating' if terminate else 'Stopping')
        logger.error(f'*** {terminate_str} the failed cluster. ***')
        # TODO(suquark): In the future we should not wait for cluster stopping
        #  or termination. This cloud speed up fail over quite a lot.
        teardown_cluster(repr(cloud),
                         region.name,
                         cluster_name,
                         terminate=terminate)
        return None
    finally:
        logger.removeHandler(fh)
        fh.close()


def teardown_cluster(cloud_name: str, region: str, cluster_name: str,
                     terminate: bool):
    provider = provision.get(cloud_name)

    if terminate:
        provider.terminate_instances(region, cluster_name)
        provision_utils.remove_cluster_profile(cluster_name)
    else:
        provider.stop_instances(region, cluster_name)
    try:
        teardown_verb = 'Terminating' if terminate else 'Stopping'
        with backend_utils.safe_console_status(
                f'[bold cyan]{teardown_verb} '
                f'[green]{cluster_name}\n'
                f'[white] Press Ctrl+C to send the task to background.'):
            if terminate:
                provider.wait_instances(region, cluster_name, 'terminated')
            else:
                provider.wait_instances(region, cluster_name, 'stopped')

    except KeyboardInterrupt:
        pass


def _post_provision_setup(cloud_name: str, cluster_name: str,
                          cluster_exists: bool,
                          handle: 'CloudVmRayBackend.ResourceHandle',
                          local_wheel_path: pathlib.Path, wheel_hash: str,
                          provision_metadata: provision_comm.ProvisionMetadata,
                          log_abs_path: str):
    # TODO(suquark): in the future, we only need to mount credentials
    #  for controllers.
    # TODO(suquark): make use of log path
    del log_abs_path

    cluster_metadata = provision.get(cluster_name).get_cluster_metadata(
        provision_metadata.region, cluster_name)
    # update launched resources
    handle.launched_resources = (handle.launched_resources.copy(
        region=provision_metadata.region, zone=provision_metadata.zone))
    handle.stable_internal_external_ips = cluster_metadata.ip_tuples()

    # TODO(suquark): Move wheel build here in future PRs.
    config_from_yaml = common_utils.read_yaml(handle.cluster_yaml)
    ip_tuples = cluster_metadata.ip_tuples()
    ip_list = [t[1] for t in ip_tuples]

    # TODO(suquark): Handle TPU VMs when dealing with GCP later.
    # if tpu_utils.is_tpu_vm_pod(handle.launched_resources):
    #     logger.info(f'{style.BRIGHT}Setting up TPU VM Pod workers...'
    #                 f'{style.RESET_ALL}')
    #     RetryingVmProvisioner._tpu_pod_setup(
    #         None, handle.cluster_yaml, handle)

    # TODO(suquark): support ssh proxy
    logger.debug(f'Waiting SSH connection for "{cluster_name}" ...')
    with backend_utils.safe_console_status(
            f'[bold cyan]Waiting SSH connection for '
            f'[green]{cluster_name}[white] ...'):
        provision_utils.wait_for_ssh([t[1] for t in ip_tuples])

    ssh_credentials = backend_utils.ssh_credential_from_yaml(
        handle.cluster_yaml)
    runners = command_runner.SSHCommandRunner.make_runner_list(
        ip_list, **ssh_credentials)

    # we mount the metadata with sky wheel for speedup
    # TODO(suquark): only mount credentials for spot controller.
    metadata_path = provision_utils.generate_metadata(cloud_name, cluster_name)
    common_file_mounts = {
        backend_utils.SKY_REMOTE_PATH + '/' + wheel_hash: str(local_wheel_path),
        backend_utils.SKY_REMOTE_METADATA_PATH: str(metadata_path),
    }
    head_node_file_mounts = {
        **common_file_mounts,
        **config_from_yaml.get('file_mounts', {})
    }

    with backend_utils.safe_console_status(
            f'[bold cyan]Mounting internal files for '
            f'[green]{cluster_name}[white] ...'):
        provision_setup.internal_file_mounts(cluster_name,
                                             common_file_mounts,
                                             head_node_file_mounts,
                                             cluster_metadata,
                                             ssh_credentials,
                                             wheel_hash=wheel_hash)

    with backend_utils.safe_console_status(
            f'[bold cyan]Running setup commands for '
            f'[green]{cluster_name}[white] ...'):
        provision_setup.internal_dependencies_setup(
            cluster_name, config_from_yaml['setup_commands'], cluster_metadata,
            ssh_credentials)

    if cluster_exists:
        with backend_utils.safe_console_status(
                f'[bold cyan]Checking Ray status for '
                f'[green]{cluster_name}[white] ...'):
            provision_setup.start_ray(runners, ip_tuples[0][0], True)
        with backend_utils.safe_console_status(
                f'[bold cyan]Checking Skylet status for '
                f'[green]{cluster_name}[white] ...'):
            provision_setup.start_skylet(runners[0])
    else:
        with backend_utils.safe_console_status(
                f'[bold cyan]Starting Ray for '
                f'[green]{cluster_name}[white] ...'):
            provision_setup.start_ray(runners, ip_tuples[0][0])
        with backend_utils.safe_console_status(
                f'[bold cyan]Starting Skylet for '
                f'[green]{cluster_name}[white] ...'):
            provision_setup.start_skylet(runners[0])
    return ip_list


def post_provision_setup(cloud_name: str, cluster_name: str,
                         cluster_exists: bool,
                         handle: 'CloudVmRayBackend.ResourceHandle',
                         local_wheel_path: pathlib.Path, wheel_hash: str,
                         provision_metadata: provision_comm.ProvisionMetadata,
                         log_dir: str):
    log_path = os.path.join(log_dir, 'post_provision_setup.log')
    log_abs_path = os.path.abspath(os.path.expanduser(log_path))
    fh = logging.FileHandler(log_abs_path)
    fh.setLevel(logging.DEBUG)
    try:
        logger.addHandler(fh)
        return _post_provision_setup(cloud_name,
                                     cluster_name,
                                     cluster_exists,
                                     handle,
                                     local_wheel_path=local_wheel_path,
                                     wheel_hash=wheel_hash,
                                     provision_metadata=provision_metadata,
                                     log_abs_path=log_abs_path)
    except Exception:  # pylint: disable=broad-except
        logger.exception('Post provision setup of cluster '
                         f'{cluster_name} failed.')
        raise
    finally:
        logger.removeHandler(fh)
        fh.close()
