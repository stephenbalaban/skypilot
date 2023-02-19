"""AWS instance provisioning."""
from typing import Dict, List, Any

import copy

import botocore

from sky import sky_logging
from sky.provision.aws import utils

BOTO_CREATE_MAX_RETRIES = 5

# Tag uniquely identifying all nodes of a cluster
TAG_RAY_CLUSTER_NAME = 'ray-cluster-name'

logger = sky_logging.init_logger(__name__)

# ======================== About AWS subnet/VPC ========================
# https://stackoverflow.com/questions/37407492/are-there-differences-in-networking-performance-if-ec2-instances-are-in-differen
# https://docs.aws.amazon.com/vpc/latest/userguide/how-it-works.html
# https://docs.aws.amazon.com/vpc/latest/userguide/configure-subnets.html

# ======================== Instance state and lifecycle ========================
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html

# ======================== About AWS availability zone ========================
# Data transfer within the same region but different availability zone
#  costs $0.01/GB:
# https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer_within_the_same_AWS_Region


def describe_instances(region: str) -> Dict:
    # overhead: 658 ms ± 65.3 ms
    return utils.create_resource('ec2', region).meta.client.describe_instances()


def _format_tags(tags: Dict[str, str]) -> List:
    return [{'Key': k, 'Value': v} for k, v in tags.items()]


def _merge_tag_specs(tag_specs: List[Dict[str, Any]],
                     user_tag_specs: List[Dict[str, Any]]) -> None:
    """Merges user-provided node config tag specifications into a base
    list of node provider tag specifications. The base list of
    node provider tag specs is modified in-place.

    This allows users to add tags and override values of existing
    tags with their own, and only applies to the resource type
    'instance'. All other resource types are appended to the list of
    tag specs.

    Args:
        tag_specs (List[Dict[str, Any]]): base node provider tag specs
        user_tag_specs (List[Dict[str, Any]]): user's node config tag specs
    """

    for user_tag_spec in user_tag_specs:
        if user_tag_spec['ResourceType'] == 'instance':
            for user_tag in user_tag_spec['Tags']:
                exists = False
                for tag in tag_specs[0]['Tags']:
                    if user_tag['Key'] == tag['Key']:
                        exists = True
                        tag['Value'] = user_tag['Value']
                        break
                if not exists:
                    tag_specs[0]['Tags'] += [user_tag]
        else:
            tag_specs += [user_tag_spec]


def _create_instances(ec2_fail_fast, cluster_name: str, node_config: Dict[str,
                                                                          Any],
                      tags: Dict[str, str], count: int) -> List:
    tags = {'Name': cluster_name, TAG_RAY_CLUSTER_NAME: cluster_name, **tags}
    conf = node_config.copy()

    tag_specs = [{
        'ResourceType': 'instance',
        'Tags': _format_tags(tags),
    }]
    user_tag_specs = conf.get('TagSpecifications', [])
    _merge_tag_specs(tag_specs, user_tag_specs)

    # SubnetIds is not a real config key: we must resolve to a
    # single SubnetId before invoking the AWS API.
    subnet_ids = conf.pop('SubnetIds')

    # update config with min/max node counts and tag specs
    conf.update({
        'MinCount': count,
        'MaxCount': count,
        'TagSpecifications': tag_specs
    })

    # NOTE: This ensures that we try ALL availability zones before
    # throwing an error.
    max_tries = max(BOTO_CREATE_MAX_RETRIES, len(subnet_ids))
    for i in range(max_tries):
        try:
            if 'NetworkInterfaces' in conf:
                # remove security group IDs previously copied from network
                # interfaces (create_instances call fails otherwise)
                conf.pop('SecurityGroupIds', None)
            else:
                # Launch failure may be due to instance type availability in
                # the given AZ. Try to always launch in the first listed subnet.
                subnet_id = subnet_ids[i % len(subnet_ids)]
                conf['SubnetId'] = subnet_id

            return ec2_fail_fast.create_instances(**conf)
        except botocore.exceptions.ClientError as exc:
            if (i + 1) >= max_tries:
                raise RuntimeError(
                    'Failed to launch instances. Max attempts exceeded.'
                ) from exc
            else:
                logger.warning(
                    f'create_instances: Attempt failed with {exc}, retrying.')


def create_instances(region: str, cluster_name: str, node_config: Dict[str,
                                                                       Any],
                     tags: Dict[str, str], count: int) -> List:
    ec2_fail_fast = utils.create_resource('ec2', region=region, max_attempts=0)
    return _create_instances(ec2_fail_fast, cluster_name, node_config, tags,
                             count)


def create_or_resume_instances(region: str, cluster_name: str,
                               node_config: Dict[str, Any],
                               tags: Dict[str, str], count: int,
                               resume_stopped_nodes: bool) -> Dict[str, Any]:
    """Creates instances.

    Returns dict mapping instance id to ec2.Instance object for the created
    instances.
    """
    ec2 = utils.create_resource('ec2', region=region)
    # sort tags by key to support deterministic unit test stubbing
    tags = dict(sorted(copy.deepcopy(tags).items()))
    filters = [
        {
            'Name': 'instance-state-name',
            'Values': ['pending', 'running', 'stopping', 'stopped'],
        },
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]
    exist_instances = list(ec2.instances.filter(Filters=filters))
    pending_instances = []
    running_instances = []
    stopping_instances = []
    stopped_instances = []

    for inst in exist_instances:
        state = inst.state['Name']
        if state == 'pending':
            pending_instances.append(inst)
        elif state == 'running':
            running_instances.append(inst)
        elif state == 'stopping':
            stopping_instances.append(inst)
        elif state == 'stopped':
            stopped_instances.append(inst)
        else:
            raise RuntimeError(f'Impossible state "{state}".')

    # TODO(suquark): Maybe in the future, users could adjust the number
    #  of instances dynamically. Then this case would not be an error.
    if resume_stopped_nodes and len(exist_instances) > count:
        raise RuntimeError('The number of running/stopped/stopping '
                           f'instances combined ({len(exist_instances)}) in '
                           f'cluster "{cluster_name}" is greater than the '
                           f'number requested by the user ({count}). '
                           'This is likely a resource leak. '
                           'Use "sky down" to terminate the cluster.')

    result = {
        'region': ec2.meta.client.meta.region_name,
        'resumed_instances': {},
        'new_instances': {}
    }
    to_start_count = count - len(running_instances) - len(pending_instances)

    if running_instances:
        result['zone'] = running_instances[0].placement['AvailabilityZone']

    if to_start_count < 0:
        raise RuntimeError('The number of running+pending instances '
                           f'({count - to_start_count}) in cluster '
                           f'"{cluster_name}" is greater than the number '
                           f'requested by the user ({count}). '
                           'This is likely a resource leak. '
                           'Use "sky down" to terminate the cluster.')

    # Try to reuse previously stopped nodes with compatible configs
    if resume_stopped_nodes and to_start_count > 0 and (stopping_instances or
                                                        stopped_instances):
        for inst in stopping_instances:
            if to_start_count <= len(stopped_instances):
                break
            inst.wait_until_stopped()
            stopped_instances.append(inst)

        resumed_instances = stopped_instances[:to_start_count]
        resumed_instances_ids = [t.id for t in resumed_instances]
        ec2.meta.client.start_instances(InstanceIds=resumed_instances_ids)
        if tags:
            # empty tags will result in error in the API call
            ec2.meta.client.create_tags(
                Resources=resumed_instances_ids,
                Tags=_format_tags(tags),
            )
        result['resumed_instances'] = {n.id: n for n in resumed_instances}
        result['zone'] = resumed_instances[0].placement['AvailabilityZone']
        to_start_count -= len(resumed_instances)

    if to_start_count > 0:
        # TODO(suquark): If there are existing instances (already running or
        #  resumed), then we cannot guarantee that they will be in the same
        #  availability zone (when there are multiple zones specified).
        #  This is a known issue before.
        new_instances = create_instances(region, cluster_name, node_config,
                                         tags, to_start_count)
        result['new_instances'] = {n.id: n for n in new_instances}
        result['zone'] = new_instances[0].placement['AvailabilityZone']

    return result


def stop_instances(region: str, cluster_name: str):
    ec2 = utils.create_resource('ec2', region=region)
    filters = [
        {
            'Name': 'instance-state-name',
            'Values': ['pending', 'running'],
        },
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]
    ec2.instances.filter(Filters=filters).stop()


def terminate_instances(region: str, cluster_name: str):
    ec2 = utils.create_resource('ec2', region=region)
    filters = [
        {
            'Name': 'instance-state-name',
            # exclude 'shutting-down' or 'terminated' states
            'Values': ['pending', 'running', 'stopping', 'stopped'],
        },
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Instance
    ec2.instances.filter(Filters=filters).terminate()


def _get_self_and_other_instances(states_filter: List[str]):
    metadata = utils.get_self_instance_metadata()
    region = metadata['region']
    self_instance_id = metadata['instance_id']
    ec2 = utils.create_resource('ec2', region=region)
    self = ec2.Instance(self_instance_id)
    tags = {}
    for t in self.tags:
        tags[t['Key']] = t['Value']
    cluster_name = tags[TAG_RAY_CLUSTER_NAME]
    filters = [
        {
            'Name': 'instance-state-name',
            'Values': states_filter,
        },
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]
    instances = ec2.instances.filter(Filters=filters)
    other_ids = []
    for inst in instances:
        if inst.id != self_instance_id:
            other_ids.append(inst.id)
    others = instances.filter(InstanceIds=other_ids)
    return self, others


def stop_instances_with_self():
    self, others = _get_self_and_other_instances(['pending', 'running'])
    others.stop()
    self.stop()


def terminate_instances_with_self():
    self, others = _get_self_and_other_instances(
        ['pending', 'running', 'stopping', 'stopped'])
    others.terminate()
    self.terminate()


def wait_instances(region: str, cluster_name: str, state: str):
    # possible exceptions: https://github.com/boto/boto3/issues/176
    ec2 = utils.create_resource('ec2', region=region)
    client = ec2.meta.client

    filters = [
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]

    if state != 'terminated':
        # NOTE: there could be a terminated AWS cluster with the same
        # cluster name.
        # Wait the cluster result in errors (cannot wait for 'terminated').
        # So here we exclude terminated instances.
        filters.append({
            'Name': 'instance-state-name',
            'Values': [
                'pending', 'running', 'shutting-down', 'stopping', 'stopped'
            ],
        })

    if state == 'running':
        waiter = client.get_waiter('instance_running')
    elif state == 'stopped':
        waiter = client.get_waiter('instance_stopped')
    elif state == 'terminated':
        waiter = client.get_waiter('instance_terminated')
    else:
        raise ValueError(f'Unsupported state to wait: {state}')
    # See https://github.com/boto/botocore/blob/develop/botocore/waiter.py
    waiter.wait(WaiterConfig={'Delay': 5, 'MaxAttempts': 120}, Filters=filters)


def get_instance_ips(region: str, cluster_name: str):
    ec2 = utils.create_resource('ec2', region=region)
    filters = [
        {
            'Name': 'instance-state-name',
            'Values': ['running'],
        },
        {
            'Name': f'tag:{TAG_RAY_CLUSTER_NAME}',
            'Values': [cluster_name],
        },
    ]
    instances = ec2.instances.filter(Filters=filters)
    # TODO: use 'Name' in inst.tags instead of 'id'
    ips = [(inst.id, (inst.private_ip_address, inst.public_ip_address))
           for inst in instances]
    return dict(sorted(ips))
