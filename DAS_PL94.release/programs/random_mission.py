"""
random_mission.py:
Generate a random mission name.
Do a quick check of the server to see if the mission already exists; if it does, make a new one.
In either event, time out after 1 second.
"""
import os
import os.path
import sys
import random
import json
import urllib
import logging
import socket
import time

try:
    import ctools.aws as aws
except ImportError:
    sys.path.append( os.path.join( os.path.dirname(__file__), ".."))
    import das_framework.ctools.aws as aws

from os.path import dirname,basename,abspath

IGNORE=True
ETC_DIR     = os.path.join( dirname(dirname(abspath(__file__))), "etc")
NOUNS       = os.path.join(ETC_DIR,'nouns.txt')
ADJECTIVES  = os.path.join(ETC_DIR,'adjectives.txt')
# Would prefer to get from das_config.json using cluster_info, but imports and pathing makes this difficult
DAS_DASHBOARD_BASE = "https://dasdashboard.ti.census.gov" if (os.getenv('DAS_ENVIRONMENT') == 'PROD') else "https://dasexperimental.ite.ti.census.gov"
CHECK_MISSION_URL = DAS_DASHBOARD_BASE+'/api/mission/{mission_name}'
MISSION_TIMEOUT = 1.0

def mission_exists(mission_name):
    """return True if the mission exists"""
    try:
        url = CHECK_MISSION_URL.format(mission_name=mission_name)
        ret = json.loads(aws.get_url(url, timeout=MISSION_TIMEOUT, ignore_cert=IGNORE))
    except (urllib.error.URLError,socket.timeout) as e:
        logging.warning(url)
        logging.warning(e)
        return []            # don't know if it exists or not, return False
    return ret

def random_line(filename):
    lines = open(filename,"r").read().split("\n")[1:]
    return random.choice(lines)

def random_mission():
    while True:
        if os.environ.get('DAS_ENVIRONMENT','')=='ITE':
            mission_name = random_line(ADJECTIVES) + " " + random_line(NOUNS)
        else:
            mission_name = time.strftime("MISSION_%Y%m%d%H%M%S")
        mission_name = mission_name.upper().replace(' ','_')
        if not mission_exists(mission_name):
            return mission_name

if __name__=="__main__":
    print(random_mission())
