#! /bin/env python3

import os
import grp
import sys
import time
import urllib.error
import json
import logging
from urllib.parse import urlencode
import boto3
import botocore


try:
    import constants as C
except ImportError as e:
    sys.path.append( os.path.join( os.path.dirname(__file__),".."))
    sys.path.append( os.path.join( os.path.dirname(__file__),"../das_framework"))
    import constants as C

from constants import CC
import das_framework.ctools.aws as aws
import das_framework.ctools.emr as emr

SANE_MAXIMUM=100
CORE_MIN=2

RUNNING_INSTANCE_COUNT='RunningInstanceCount'
REQUESTED_INSTANCE_COUNT='RequestedInstanceCount'

def boto3_config():
    return botocore.config.Config(
        proxies={'https':os.environ['BCC_HTTPS_PROXY'].replace("https://","")})

def user_in_group(user, group):
    try:
      users = grp.getgrnam(group).gr_mem
    except KeyError as e:
      users = []
    return (user in users)

def getInstanceGroups():
    emr_client = boto3.client('emr', config = boto3_config())
    return emr_client.list_instance_groups(ClusterId=emr.clusterId())['InstanceGroups']

def print_info():
    for ig in getInstanceGroups():
        print("{}  requested: {}  running: {}".format(ig['Name'],ig[REQUESTED_INSTANCE_COUNT],ig[RUNNING_INSTANCE_COUNT]))

def requestInstanceCounts(core, task, background=False, dry_run=False):
    """
    :param core: the number of Core nodes requested, or None to note change
    :param task: the number of task nodes requested, or None to not change.
    :param background: if true, request the resize in the background (immediately return)
    :param dry_run: if true, return True if the request would cause a resize, otherwise return false
    """

    cmd            = ['modify-instance-groups','--instance-groups']
    instanceGroups = []
    willResize     = False
    for ig in getInstanceGroups():
        if "core" in ig['Name'].lower() and (core is not None):
            if core > SANE_MAXIMUM:
                raise ValueError("May not request more than %d core and task nodes" % SANE_MAXIMUM)
            if core < CORE_MIN:
                raise ValueError(f"Must request at least {CORE_MIN} CORE instances (requested {core})")
            if core!=ig[RUNNING_INSTANCE_COUNT]:
                willResize = True

            instanceGroups.append({'InstanceGroupId':ig['Id'],
                                   'InstanceCount':int(core),
                                   'ShrinkPolicy':{}})
        if "task" in ig['Name'].lower() and (task is not None):
            if (task > SANE_MAXIMUM) or (core is not None and task+core > SANE_MAXIMUM):
                raise ValueError("May not request more than %d core and task nodes" % SANE_MAXIMUM)
            if task!=ig[RUNNING_INSTANCE_COUNT]:
                willResize = True

            instanceGroups.append({'InstanceGroupId':ig['Id'],
                                   'InstanceCount':int(task),
                                   'ShrinkPolicy':{}})

    if (not dry_run) and willResize:
        if background:
            """Running in the background (this is slow), so fork"""
            if os.fork()!=0:
                return
        emr_client = boto3.client('emr', config = boto3_config())
        emr_client.modify_instance_groups(
            ClusterId=emr.clusterId(),
            InstanceGroups=instanceGroups)
    return willResize

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Resize the cluster. Note that it takes 5-10 minutes to spin up new nodes." )
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--core", type=int)
    parser.add_argument("--task", type=int)
    parser.add_argument("--bg", action="store_true", help="Run in background and do not print the output")
    args = parser.parse_args()

    willResize = False
    if args.task is not None or args.core is not None:
        willResize = requestInstanceCounts(args.core,args.task)

    if args.bg is False:
        print("clusterId:",emr.clusterId(),file=sys.stderr)
        print_info()
        if willResize:
            print("RESIZING")
