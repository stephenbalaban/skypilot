"""skylet events"""
import getpass
import math
import os
import re
import subprocess
import sys
import time
import traceback

import psutil
import yaml

from sky import sky_logging
from sky.backends import backend_utils, cloud_vm_ray_backend
from sky.skylet import autostop_lib, job_lib
from sky.spot import spot_utils
from sky.utils import common_utils

# Seconds of sleep between the processing of skylet events.
EVENT_CHECKING_INTERVAL_SECONDS = 20
logger = sky_logging.init_logger(__name__)


class SkyletEvent:
    """Skylet event.

    The event is triggered every EVENT_INTERVAL_SECONDS seconds.

    Usage: override the EVENT_INTERVAL_SECONDS and _run method in subclass.
    """
    # Run this event every this many seconds.
    EVENT_INTERVAL_SECONDS = -1

    def __init__(self):
        self._event_interval = int(
            math.ceil(self.EVENT_INTERVAL_SECONDS /
                      EVENT_CHECKING_INTERVAL_SECONDS))
        self._n = 0

    def run(self):
        self._n = (self._n + 1) % self._event_interval
        if self._n % self._event_interval == 0:
            logger.debug(f'{self.__class__.__name__} triggered')
            try:
                self._run()
            except Exception as e:  # pylint: disable=broad-except
                # Keep the skylet running even if an event fails.
                logger.error(f'{self.__class__.__name__} error: {e}')
                logger.error(traceback.format_exc())

    def _run(self):
        raise NotImplementedError


class JobUpdateEvent(SkyletEvent):
    """Skylet event for updating job status."""
    EVENT_INTERVAL_SECONDS = 300

    # Only update status of the jobs after this many seconds of job submission,
    # to avoid race condition with `ray job` to make sure it job has been
    # correctly updated.
    # TODO(zhwu): This number should be tuned based on heuristics.
    _SUBMITTED_GAP_SECONDS = 60

    def _run(self):
        job_owner = getpass.getuser()
        job_lib.update_status(job_owner,
                              submitted_gap_sec=self._SUBMITTED_GAP_SECONDS)


class SpotJobUpdateEvent(SkyletEvent):
    """Skylet event for updating spot job status."""
    EVENT_INTERVAL_SECONDS = 300

    def _run(self):
        spot_utils.update_spot_job_status()


class AutostopEvent(SkyletEvent):
    """Skylet event for autostop.

    Idleness timer gets set to 0 whenever:
      - A first autostop setting is set. By "first", either there's never any
        autostop setting set, or the last autostop setting is a cancel (idle
        minutes < 0); or
      - This event wakes up and job_lib.is_cluster_idle() returns False; or
      - The cluster has restarted; or
      - A job is submitted (handled in the backend; not here).
    """
    EVENT_INTERVAL_SECONDS = 60

    _UPSCALING_PATTERN = re.compile(r'upscaling_speed: (\d+)')
    _CATCH_NODES = re.compile(r'cache_stopped_nodes: (.*)')

    def __init__(self):
        super().__init__()
        autostop_lib.set_last_active_time_to_now()
        self._ray_yaml_path = os.path.abspath(
            os.path.expanduser(backend_utils.SKY_RAY_YAML_REMOTE_PATH))

    def _run(self):
        autostop_config = autostop_lib.get_autostop_config()

        if (autostop_config.autostop_idle_minutes < 0 or
                autostop_config.boot_time != psutil.boot_time()):
            autostop_lib.set_last_active_time_to_now()
            logger.debug('autostop_config not set. Skipped.')
            return

        if job_lib.is_cluster_idle():
            idle_minutes = (time.time() -
                            autostop_lib.get_last_active_time()) // 60
            logger.debug(
                f'Idle minutes: {idle_minutes}, '
                f'AutoStop config: {autostop_config.autostop_idle_minutes}')
        else:
            autostop_lib.set_last_active_time_to_now()
            idle_minutes = -1
            logger.debug(
                'Not idle. Reset idle minutes.'
                f'AutoStop config: {autostop_config.autostop_idle_minutes}')
        if idle_minutes >= autostop_config.autostop_idle_minutes:
            logger.info(
                f'{idle_minutes} idle minutes reached; threshold: '
                f'{autostop_config.autostop_idle_minutes} minutes. Stopping.')
            self._stop_cluster(autostop_config)

    def _stop_cluster(self, autostop_config):
        if (autostop_config.backend ==
                cloud_vm_ray_backend.CloudVmRayBackend.NAME):
            autostop_lib.set_autostopping_started()
            self._replace_yaml_for_stopping(self._ray_yaml_path,
                                            autostop_config.down)

            # `ray up` is required to reset the upscaling speed and min/max
            # workers. Otherwise, `ray down --workers-only` will continuously
            # scale down and up.
            logger.info('Running ray up.')
            script = (cloud_vm_ray_backend.
                      write_ray_up_script_with_patched_launch_hash_fn(
                          self._ray_yaml_path,
                          ray_up_kwargs={'restart_only': True}))
            subprocess.run(
                [sys.executable, script],
                check=True,
                # Use environment variables to disable the ray usage collection
                # (to avoid overheads and potential issues with the usage)
                # as sdk does not take the argument for disabling the usage
                # collection.
                env=dict(os.environ, RAY_USAGE_STATS_ENABLED='0'),
            )

            logger.info('Running ray down.')
            # Stop the workers first to avoid orphan workers.
            subprocess.run(
                ['ray', 'down', '-y', '--workers-only', self._ray_yaml_path],
                check=True)

            logger.info('Running final ray down.')
            subprocess.run(['ray', 'down', '-y', self._ray_yaml_path],
                           check=True)
        else:
            raise NotImplementedError

    def _replace_yaml_for_stopping(self, yaml_path: str, down: bool):
        with open(yaml_path, 'r') as f:
            yaml_str = f.read()
        yaml_str = self._UPSCALING_PATTERN.sub(r'upscaling_speed: 0', yaml_str)
        if down:
            yaml_str = self._CATCH_NODES.sub(r'cache_stopped_nodes: false',
                                             yaml_str)
        else:
            yaml_str = self._CATCH_NODES.sub(r'cache_stopped_nodes: true',
                                             yaml_str)
        config = yaml.safe_load(yaml_str)
        # Set the private key with the existed key on the remote instance.
        config['auth']['ssh_private_key'] = '~/ray_bootstrap_key.pem'
        # NOTE: We must do this, otherwise with ssh_proxy_command still under
        # 'auth:', `ray up ~/.sky/sky_ray.yaml` on the head node will fail (in
        # general, the clusters do not need or have the proxy set up).
        #
        # Note also that this is ok only because in the local client ->
        # provision head node code path, we have monkey patched
        # hash_launch_conf() to exclude ssh_proxy_command from the hash
        # calculation for the head node. Therefore when this current code is
        # run again on the head, the hash would match the one at head's
        # creation (otherwise the head node would be stopped and a new one
        # would be launched).
        config['auth'].pop('ssh_proxy_command', None)
        # Empty the file_mounts.
        config['file_mounts'] = dict()
        common_utils.dump_yaml(yaml_path, config)
        logger.debug('Replaced upscaling speed to 0.')
