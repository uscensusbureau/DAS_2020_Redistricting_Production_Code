#!/usr/bin/env python3
#
# cluster_info.py:
# A module of useful EMR cluster management tools.
# We've had to build our own to work within the Census Environment
# This script appears in:
#   das-vm-config/bin/cluster_info.py
#   emr_stats/cluster_info.py
#
# Currently we manually sync the two; perhaps it should be moved to ctools.

from pathlib import Path
from subprocess import Popen, PIPE, call, check_call, check_output
import json
import logging
import multiprocessing
import os
import subprocess
import sys
import time
import urllib.request

from os.path import abspath,dirname,basename

# Beware!  An error occurred (ThrottlingException) in complete_cluster_info()
# when calling the ListInstances operation (reached max retries: 4): Rate exceeded
#
# We experienced throttling with DEFAULT_WORKERS=20
#
# So we use 4
DEFAULT_WORKERS=4

# We also now implement exponential backoff
MAX_RETRIES = 7
RETRY_MS_DELAY = 50


debug = False

# Bring in ec2. It's either in the current directory, or its found through
# the ctools.ec2 module

try:
    import ec2
except ImportError as e:
    try:
        sys.path.append(os.path.dirname(__file__))
        import ec2
    except ImportError as e:
        raise RuntimeError("Cannot import ec2")

# Proxy is controlled in aws

_isMaster  = 'isMaster'
_isSlave   = 'isSlave'
_clusterId = 'clusterId'
_diskEncryptionConfiguration='diskEncryptionConfiguration'
_encryptionEnabled='encryptionEnabled'

Status='Status'


def show_credentials():
    subprocess.call(['aws', 'configure', 'list'])

def get_url(url):
    with urllib.request.urlopen(url) as response:
        return response.read().decode('utf-8')

def decode_user_data(user_data_raw):
    """Decode the raw user data provided by the Amazon API. Previously this was a JSON value; now it is a base64-encoded GZIP file inside a YAML value."""
    try:
        return json.loads(user_data_raw)
    except json.decoder.JSONDecodeError as e:
        pass
    try:
        import yaml
        import gzip
        import base64
        # In later EMR version Amazon moved to distributing this as a YAML file
        y = yaml.load(user_data_raw, Loader=yaml.SafeLoader)
        return json.loads(gzip.decompress(base64.b64decode(y['write_files'][0]['content'])))
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        raise RuntimeError("Cannot find user_data in YAML file")

def user_data():
    """user_data is only available on EMR nodes. Otherwise we get an
    error, which we turn into a FileNotFound error"""
    try:
        user_data_raw = get_url("http://169.254.169.254/2016-09-02/user-data/")
        if user_data_raw.startswith("#!"):
            raise FileNotFoundError("user-data is only available in EMR")
    except urllib.error.URLError as e:
        raise FileNotFoundError("user-data is only available in EMR")
    return decode_user_data(user_data_raw)


def releaseLabel():
    return json.loads(open("/emr/instance-controller/lib/info/extraInstanceData.json", "r").read())['releaseLabel']

def encryptionEnabled():
    return user_data()['diskEncryptionConfiguration']['encryptionEnabled']

def isMaster():
    """Returns true if running on master"""
    return user_data()['isMaster']

def isSlave():
    """Returns true if running on master"""
    return user_data()['isSlave']

def decode_status(meminfo):
    return {line[:line.find(":")]: line[line.find(":") +1:].strip() for line in meminfo.split("\n")}

def clusterId():
    return user_data()['clusterId']

# https://docs.aws.amazon.com/general/latest/gr/api-retries.html
def aws_emr_cmd(cmd, retries=MAX_RETRIES, decode=True):
    """run the command and return the JSON output. implements retries"""
    for retries in range(retries):
        try:
            rcmd = ['aws', 'emr', '--output', 'json'] + cmd
            if debug:
                print(f"aws_emr_cmd pid{os.getpid()}: {rcmd}")
            p = subprocess.Popen(rcmd, encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = p.communicate()
            if p.poll()!=0:
                raise subprocess.CalledProcessError(f"p={p.poll()})")
            if decode:
                return json.loads(out)
            else:
                return p.poll()
        except subprocess.CalledProcessError as e:
            delay = (2**retries * RETRY_MS_DELAY /1000)
            logging.warning(f"aws emr subprocess.CalledProcessError. "
                            f"Retrying count={retries} delay={delay}")
            time.sleep(delay)
        except (json.decoder.JSONDecodeError) as e:
            logging.error(f"JSONDecodeError: out: {out}  err: {err}")
            raise e
    raise e


def list_clusters(*, state=None):
    """Returns the AWS Dictionary of cluster information"""
    cmd = ['list-clusters']
    if state is not None:
        cmd += ['--cluster-states', state]
    data = aws_emr_cmd(cmd)
    return data['Clusters']

def describe_cluster(clusterId):
    data = aws_emr_cmd(['describe-cluster', '--cluster', clusterId])
    return data['Cluster']

def list_instances(clusterId=None):
    if clusterId is None:
        clusterId = clusterId()
    data = aws_emr_cmd(['list-instances', '--cluster-id', clusterId])
    return data['Instances']

def add_cluster_info(cluster):
    clusterId = cluster['Id']
    cluster['describe-cluster'] = describe_cluster(clusterId)
    cluster['instances']        = list_instances(clusterId)
    cluster['terminated']       = 'EndDateTime' in cluster['Status']['Timeline']
    # Get the id of the master
    try:
        masterPublicDnsName = cluster['describe-cluster']['MasterPublicDnsName']
        masterInstance = [i for i in cluster['instances'] if i['PrivateDnsName']==masterPublicDnsName][0]
        masterInstanceId = masterInstance['Ec2InstanceId']
        # Get the master tags
        cluster['MasterInstanceTags'] = {}
        for tag in ec2.describe_tags(resourceId=masterInstanceId):
            cluster['MasterInstanceTags'][tag['Key']] = tag['Value']
    except KeyError as e:
        pass
    return cluster

def complete_cluster_info(workers=DEFAULT_WORKERS, terminated=False):
    """Pull all of the information about all the clusters efficiently using the
    EMR cluster API and multithreading. If terminated=True, get
    information about the terminated clusters as well.
    """
    clusters = list_clusters()
    for cluster in list(clusters):
        if terminated==False and cluster['Status']['State']=='TERMINATED':
            clusters.remove(cluster)
    with multiprocessing.Pool(workers) as p:
        clusters = p.map(add_cluster_info, clusters)

    return clusters


if __name__=="__main__":
    print("user data test: ", user_data())
