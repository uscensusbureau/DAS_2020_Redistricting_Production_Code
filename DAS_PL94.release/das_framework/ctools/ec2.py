import json
import subprocess
import boto3


Ec2InstanceId='Ec2InstanceId'
Status='Status'

def create_tag(*, resourceId, key_value_pairs):
    cmd = ['aws', 'ec2', 'create-tags', '--resources', resourceId]
    for (key, value) in key_value_pairs:
        cmd += ['--tags', f'Key={key},Value={value}']
    subprocess.check_call(cmd)

def delete_tag(*, resourceId, tags):
    cmd = ['aws', 'ec2', 'delete-tags', '--resources', resourceId]
    for tag in tags:
        cmd += ['--tags', f'Key={tag}']
    subprocess.check_call(cmd)

def describe_tags(*, resourceId):
    cmd = ['aws', 'ec2', 'describe-tags', '--filters', f'Name=resource-id,Values={resourceId}', '--output', 'json', '--no-paginate']
    return json.loads(subprocess.check_output(cmd))['Tags']


def describe_instances(*, groupid=None):
    cmd = ['aws', 'ec2', 'describe-instances', '--output', 'json']
    if groupid:
        cmd += ['--filters', f'Name=instance.group-id,Values={groupid}']
    return sum([reservation['Instances'] for reservation in json.loads(subprocess.check_output(cmd))['Reservations']], [])

def get_instance_tags(instanceId=None):
    """Return a dictionary of all the tags for a given instance, default this instance."""
    import aws
    if instanceId is None:
        instanceId = aws.instanceId()
    with aws.Proxy(https=True, http=False) as p:
        ec2_client = boto3.client('ec2')
        response = ec2_client.describe_instances(InstanceIds=[aws.instanceId()])
        taglist = response['Reservations'][0]['Instances'][0]['Tags']
        return {d['Key']: d['Value'] for d in taglist}


if __name__=="__main__":
    print("Instance tags:\n", json.dumps(get_instance_tags(), indent=4))
