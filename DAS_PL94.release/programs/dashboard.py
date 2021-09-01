"""
Dashboard connector.

A collection of tools for sending information to the dashboard for the mission log.

- Python functions for sending JSON objects representing mission log to the dashboard.
  They can be sent three ways:
   1 - REST to the dashboard (free)
   2 - SQS messages.  Cost: first million requests free; 1 M - 100B, $0.40/million.
   3 - Storing them in S3. (cost: s3 ops)

    - Note SQS supports 300 API calls/sec, which gives you 3000 transactions/sec with batching.
      However, they is high latency, so we batch in a different thread.
      We debugged all of this SQS stuff, and then decided to use it as a fallback when REST isn't available.

- Command-line for reading from stdin and sending to dashboard and optionally to a file

Use missionlog.py for viewing the mission in real time.

if 'use_debug_url' is set in the object, use the debug URL below.

You can force the system to not use REST with 'export TRY_REST_FIRST=NO'
You can force the system not to use SQS with 'export TRY_SQS_SECOND=NO'
To force REST to use POST, 'export FORCE_REST_POST=YES'

"""

import os
import sys
import time
import urllib
import urllib.error
import urllib.request
import json
import logging
import uuid
import boto3
import botocore.config
import socket
import queue
import threading
import atexit
import select
import datetime

# These are hard coded for now. Sorry!
# das_log endpoints
# Would prefer to get from das_config.json using cluster_info, but imports and pathing makes this difficult
DAS_DASHBOARD_BASE = "https://dasdashboard.ti.census.gov" if (os.getenv('DAS_ENVIRONMENT') == 'PROD') else "https://dasexperimental.ite.ti.census.gov"

DAS_LOG_ENDPOINT       = f"{DAS_DASHBOARD_BASE}/api/daslog/send"
DAS_LOG_ENDPOINT_DEBUG = "https://dasexperimental.ite.ti.census.gov/~garfi303adm/html/api/daslog/send"

# endpoints for getting mission information
DAS_MISSION_API    = "/api/mission/{identifier}"
DAS_MISSION_LOG    = "/api/mission/{identifier}/log?last={last}"

USE_DEBUG_URL = 'use_debug_url'

MAX_LINES_TO_DASHBOARD = 20000

# Set DEBUG to TRUE to force using DEBUG_URL

DEBUG=False

CODE_HEARTBEAT  =  0
CODE_STDOUT     =  1
CODE_STDERR     =  2
CODE_ANNOTATE   = 10
CODE_ALERT      = 15
CODE_STACKTRACE = 20
CODE_LIST = [name for name in globals() if name.startswith("CODE_")]

DASLOG_SENDER = 'daslog'


# Easy way to get observable global variables.
# By default, always try REST first. But if we fail, turn YES to NO.
# Because it is in the environment, we can observe this using the `ps` command.
TRY_REST_FIRST='TRY_REST_FIRST'
TRY_SQS_SECOND='TRY_SQS_SECOND'
FORCE_REST_POST='FORCE_REST_POST'
FORCE_DEBUG_URL='FORCE_DEBUG_URL'
USE_DEBUG_URL='USE_DEBUG_URL'
YES = 'YES'
NO  = 'NO'

FAKE_RETRY_VALUE=0.0123


# from hashlib import md5
from urllib.parse import urlencode,urlparse
from os.path import dirname,basename,abspath

START_TIME = time.time()

# Make sure we can import relative to das_framework, which is the parent directory
PARENT_DIR = dirname( dirname( abspath( __file__ )))
if PARENT_DIR not in sys.path:
    sys.path.append( PARENT_DIR )

# import constants as C
from constants import CC
import programs.colors as colors
import das_framework.ctools as ctools
import das_framework.ctools.aws as aws
import das_framework.ctools.env as env

try:
    import programs.random_mission as random_mission
except ImportError as e:
    import random_mission


# https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

################################################################
# Code to pass configuration information to workers
# JCR is the JSONConfigReader that reads the configuration information from das_config.json
# Note that we can't instantiate this on the workers on import because the DAS_ENVIRONMENT variable
# isn't set up at that point. That's why it is a singleton
class JCR(metaclass=Singleton):
    def __init__(self):
        self.jcr = env.JSONConfigReader(path=CC.DAS_CONFIG_JSON, envar='DAS_ENVIRONMENT')
    def get_config(self, var):
        return self.jcr.get_config(var)

# URLs we use

def das_sqs_url():
    return JCR().get_config('DAS_SQS.URL')

def bcc_https_proxy():
    return JCR().get_config('BCC_HTTPS_PROXY')

def das_object_cache_loc():
    return os.path.join(JCR().get_config('DAS_S3MGMT'), JCR().get_config('DAS_OBJECT_CACHE_BASE'))


def das_object_cache_acl():
    return JCR().get_config('DAS_S3MGMT_ACL')

################################################################
## Ultra-simple mechanism for sending messages to the dashboard via S3
def send_message_s3(message):
    """fallback to save a message in an S3 destination, which always works.
    The S3 object name is just a uuid4().
    """

    p = urlparse(das_object_cache_loc() + "/" + str(uuid.uuid4()))

    bucket = p.netloc
    key    = p.path[1:]
    boto3.resource('s3').Object(bucket, key).put(Body=message, ACL=das_object_cache_acl())


##
################################################################

#
# We use a worker running in another thread to collect SQS messages
# and send them asychronously to the SQS in batches of 10 (or when WATCH_TIME expires.)
#

def sqs_queue():
    return boto3.resource('sqs',
                          config = botocore.config.Config(
                              proxies={'https':bcc_https_proxy().replace("https://","")}
                          )).Queue(das_sqs_url())

SQS_MAX_MESSAGES=10             # SQS allows sending up to 10 messages at a time
WATCHER_TIME=5                  # how long to collect SQS messages before sending them
EXIT='exit'                     # token message to send when program exits


# SQS Worker. Collects
class SQS_Client(metaclass=Singleton):
    """SQS_Client class is a singleton.
    This uses a python queue to batch up messages that are send to the AWS Quwue.
    We batch up to 10 messages a time, but send every message within 5 seconds.
    """
    def __init__(self):
        """Set up the singleton by:
        - getting a handle to the SQS queue through the BCC proxy.
        - Creating the python queue for batching the requests to the SQS queue.
        - Creating a background process to flush the queue every 5 seconds.
        """
        # Set the default
        if TRY_SQS_SECOND not in os.environ:
            os.environ[TRY_SQS_SECOND]=YES

        self.sqs_queue = sqs_queue()  # queue to send this to SQS
        self.pyqueue  = queue.Queue() # producer/consumer queue used by dashboard.py
        self.worker = threading.Thread(target=self.watcher, daemon=True)
        self.worker.start()
        atexit.register(self.terminate)

    def flush(self, timeout=0.0):
        """Flush the pyqueue. Can be called from the main thread or the watcher thread.
        While there are messages in the queue, grab up to 10, then send them to the sqs_queue.
        Returns last message processed, which may be EXIT.
        The watcher repeatedly calls flush() until it receives an Exit.
        """
        entries = []
        msg     = None
        t0      = time.time()
        while True:
            try:
                msg = self.pyqueue.get(timeout=timeout, block=True)
            except queue.Empty as e:
                break
            if msg==EXIT:
                break
            msg['Id'] = str( len( entries ))
            entries.append(msg)
            if len(entries)==SQS_MAX_MESSAGES:
                break
            if time.time() - t0 > timeout:
                break
        if entries:
            # Send the 1-10 messages.
            # If this fails, just save them in S3.
            try:
                if os.getenv(TRY_SQS_SECOND)==YES:
                    self.sqs_queue.send_messages(Entries=entries)
                    entries = []
            except botocore.exceptions.ClientError as err:
                logging.warning("Cannot send by SQS; sending by S3")
                os.environ[TRY_SQS_SECOND]=NO
        if entries:
            assert os.getenv(TRY_SQS_SECOND)==NO # should have only gotten here if we failed above
            for entry in entries:
                send_message_s3(entry['MessageBody'])
        return msg

    def watcher(self):
        """Repeatedly call flush().
        If the flush gets exit, it returns EXIT and we EXIT.
        """
        while True:
            if self.flush(timeout=WATCHER_TIME)==EXIT:
                return

    def queue_message(self, *, MessageBody, **kwargs):
        self.pyqueue.put({'MessageBody':MessageBody})

    def terminate(self):
        """Tell the watcher to exit"""
        self.flush()
        self.pyqueue.put(EXIT)


################################################################
## Support
################################################################

def get_mission_name():
    """If a mission name is not in the global environment,
    get one from the server and put it in the global environment.
    Otherwise generate a random mession name and put it in the global environment.
    """
    if CC.MISSION_NAME not in os.environ:
        os.environ[CC.MISSION_NAME] = random_mission.random_mission()
    return os.environ[CC.MISSION_NAME]

def make_int(s):
    if s.lower().endswith('g'):
        return int(s[0:-1]) * 1024*1024*1024
    elif s.lower().endswith('m'):
        return int(s[0:-1]) * 1024*1024
    elif s.lower().endswith('k'):
        return int(s[0:-1]) * 1024
    else:
        return int(s)

def str_to_code(s):
    """Given a variable name in the global namespace, return what its value is"""
    return globals()[s]

################################################################
## Sending objs to the dashboard.
################################################################


def send_url(url, post=None):
    """Very safe way to send a URL without crashing. Returns True if URL was successfully received.
    :param url: the URL to send
    :param post: a dictionary to post.
    """
    try:
        if post is None:
            data = None
        else:
            data = urllib.parse.urlencode(post).encode()
        with urllib.request.urlopen(url, data=data, timeout = 1.0) as response:
            return response.code==200
    except urllib.error.URLError as e:
        logging.error("send_url urllib.error.URLError: %s  url: %s ",e,url)
        return None
    except urllib.error.HTTPError as e:
        if e.args[0]==414:
            print("URL too long. Resending with post")
        logging.error("send_url urllib.error.HTTPError: %s  url: %s",e,url)
        return None
    except RuntimeError as e:
        logging.error("send_url url: %s RuntimeError: %s",url,e)
        return None
    except socket.timeout as e:
        logging.error("socket.timeout. url: %s",url)
        return None

def send_obj(*, obj, sender=DASLOG_SENDER):
    """Send an object to a URL with GET encoding.
    This code also runs on the Spark workers to send data to the dashboard for Gurobi license retries.
    """

    if 'instanceId' not in obj:
        if CC.INSTANCEID not in os.environ:
            os.environ[CC.INSTANCEID] = aws.instanceId()
        obj['instanceId']   = os.environ[CC.INSTANCEID]

    try:
        obj['das_run_uuid'] = os.environ[CC.DAS_RUN_UUID]
    except KeyError as e:
        print("*** DAS_RUN_UUID not in environment.",file=sys.stderr)
        print("*** Please modify your run script to add it.",file=sys.stderr)
        raise RuntimeError("DAS_RUN_UUID not in environment")

    obj['ipaddr']       = socket.gethostbyname(socket.gethostname())
    obj['t']            = str(datetime.datetime.now())

    if TRY_REST_FIRST not in os.environ:
        os.environ[TRY_REST_FIRST] = YES

    if os.getenv(TRY_REST_FIRST) == YES:
        ## First try REST if we have a URL.
        ## We encode the whole URL as the GET string.  It might be big, but probably not beyond 4k.
        if (USE_DEBUG_URL in obj) or (os.getenv(FORCE_DEBUG_URL)==YES):
            url = DAS_LOG_ENDPOINT_DEBUG
        else:
            url = DAS_LOG_ENDPOINT

        surl = url + "?" + urlencode(obj)
        if (len(surl) < 8000) and (os.getenv(FORCE_REST_POST)!=YES):
            # If URL is < 8000 characters, we can send it with get (preferred).
            # We prefer this because it's nice to see what was sent in the Apache logfile.
            # Note that if we were sending PII, we would feel quite differently!
            r = send_url(surl)
        else:
            # URL>8000 characters, we need to send via post.
            surl = url
            r = send_url(surl, post=obj)
        if 'debug' in obj:
            print(f"surl={surl} r={r}",file=sys.stderr)
        if r:
            return
        os.environ[TRY_REST_FIRST] = 'NO'

    ## That didn't work. Add it to the queue. It will be sent either by SQS or S3
    ## That uses a work queue, so it will return very fast.
    ## REST is fast enough that we didn't need a work queue.
    message = json.dumps({'sender':sender, 'data':obj})
    SQS_Client().queue_message(MessageBody = message)


def token_retry(*, debug=False, use_debug_url=False, retry, delay, success):
    """Send a message that the token is being retried. This is run on the
    workers, which means that each worker has the ability to create an
    SQS work queue, to send SQS messages, send REST messages, and put things in S3.

    """
    if DEBUG:
        debug = True
        logging.basicConfig(level=logging.DEBUG)

    obj = {'attempt': str(retry),
           'delay'  : str(delay),
           'success': str(success),
           't'      : str(datetime.datetime.now()),
           'token_retry': True}

    if use_debug_url:
        obj[USE_DEBUG_URL] = True

    send_obj(obj=obj)

def das_log(message=None, verbose=False, debug=False, log_spark=False, log_gurobi=True, code=CODE_ANNOTATE, extra={}):
    """Send a message to the dassexperimental server using REST API.
    Each DAS RUN has a run_id. This needs to be provided. If the global variable hasn't been set,
    then we assume that this is a new run and we get the newrun parameter. Other parameters are added if we can learn them.

    if message is not None, that message is appended to das_log.
    If message is none, then only das_runs table is updated.

    :param message:   if not None, then this message is appended to the das_log table
    :param verbose:   display message on stdout
    :param debug:     prints details of RPC operation
    :param log_spark: Log spark-specific information in das_runs. This only needs to be done once, after the DAS run is finished
    :param code:      Log level; we use our own, rather than syslog values
    :param extra:     additional key/value pairs to send to CGI server.
                      They will be recorded in das_runs if there are matching column names.
    """

    if verbose:
        t = time.time() - START_TIME
        seconds  = int(t)
        hundreds = int((t-seconds)*100)
        print(f"DAS_LOG: t={seconds}.{hundreds:02} {message} (🡒DASHBOARD)")

    # We create a JSON object that contains the information to log.
    # Figure out what goes in it.

    obj = {**{'clusterId':os.getenv(CC.CLUSTERID_ENV),
              'instanceId': aws.instanceId(),
              'code' : code },
           **extra}

    if debug:
        obj['debug'] = debug

    if verbose:
        obj['verbose'] = verbose

    # The message to log
    if message:
        obj['message'] = message

    # Any environment variables
    for var in ['JBID','DAS_STEP']:
        if var in os.environ:
            obj[ var.lower() ] = os.environ[ var ]

    # Spark info, if requested
    if log_spark:
        from pyspark.sql import SparkSession
        conf = SparkSession.builder.getOrCreate().sparkContext.getConf()
        def getSparkOption(conf, spark_option):
            option = conf.get(spark_option)
            if option is None:
                return None
            return make_int(option)
        obj['driver_memory']   = getSparkOption(conf, 'spark.driver.memory')
        obj['executor_memory'] = getSparkOption(conf, 'spark.executor.memory')
        obj['executor_cores']  = getSparkOption(conf, 'spark.executor.cores')
        obj['num_executors']   = getSparkOption(conf, 'spark.executor.instances')
        obj['max_result_size'] = getSparkOption(conf, 'spark.driver.maxResultSize')

    # Gurobi info, if requested.
    if log_gurobi:
        import gurobipy as gb
        obj['gurobi_version'] = ".".join((map(str,gb.gurobi.version())))
        obj['python_executable'] = sys.executable

    # Mission_name, if known
    if (CC.MISSION_NAME in os.environ) and ('mission_name' not in obj):
        obj['mission_name'] = os.environ[CC.MISSION_NAME]

    send_obj(obj=obj, sender = DASLOG_SENDER)

def das_tee(*, args):
    """Take input from STDIN and send it to the dashboard.
    :param code: which channel to send it to.
    """
    code = str_to_code(args.code)

    outfiles = []
    if args.stderr:
        outfiles.append(sys.stderr)
    else:
        outfiles.append(sys.stdout)
    if args.write:
        try:
            outfiles.append( open( args.write, 'w'))
        except FileNotFoundError as e:
            print(f"Cannot open {args.write} for writing",file=sys.stderr)
            print(f"Full path: {os.path.abspath(args.write)}", file=sys.stderr)
            print(f"Current directory: {os.getcwd()}",file=sys.stderr)
            raise e

    lines = 0
    MAXLINES = 10
    TIMEOUT  = 5.0
    while True:
        # Get all of the pending data up to MAXLINES. Don't wait more than 5 seconds
        linebuf = []
        t0 = time.time()
        while select.select([sys.stdin,],[],[], TIMEOUT)[0]:
            line = sys.stdin.readline()
            if len(line)==0:
                exit(0)          # end of input file
            linebuf.append(line)
            lines += 1
            if len(linebuf)>MAXLINES or (time.time()-t0)>TIMEOUT:
                break

        if not linebuf:
            continue

        if lines < MAX_LINES_TO_DASHBOARD:
            obj = {'message':"\n".join(linebuf),
                   'code':code}
            send_obj(obj = obj)

        if args.red:
            sol = colors.CRED+colors.CWHITEBG2
            eol = colors.CEND
        else:
            sol = ''
            eol = ''

        for out in outfiles:
            out.write(sol)
            for (ct,line) in enumerate(linebuf):
                out.write(f"{lines-len(linebuf)+ct:06d} {line}")
            out.write(eol)
            out.flush()


def heartbeat(message=None, code=CODE_HEARTBEAT):
    """Tell the server we are still alive if we are not running in lights out mode. This may be
run in another thread. No locking is required on os.enviorn[] because
of the GIL"""
    if os.getenv(CC.DAS_LIGHTS_OUT)!='TRUE':
        das_log(message=message, code=code, extra={'heartbeat':1})


def fake_das_run_uuid():
    return 'xxxxxxxx' + str(uuid.uuid4())[9:]

def mission_start(obj, args):
    """"
    Creates the DAS_RUN_UUID.
    Creates the MISSION_NAME if it is not already set.
    Creates the LOGFILE_NAME.
    Sends object to the dashboard.
    param obj: the object so far.
    """

    # DAS_RUN_UUID
    if CC.DAS_RUN_UUID in os.environ:
        raise RuntimeError(f"{CC.DAS_RUN_UUID} already in os.environ.")
    os.environ[CC.DAS_RUN_UUID] = das_run_uuid = str(uuid.uuid4())

    # MISSION_NAME
    mission_name = get_mission_name()
    if CC.MISSION_NAME not in os.environ:
        os.environ[CC.MISSION_NAME] = mission_name

    # LOGFILE_NAME
    now = datetime.datetime.now()
    minutes_since_midnight = now.hour*60+now.minute
    LOGFILE_NAME=f"logs/DAS-{now.year:04}-{now.month:02}-{now.day:02}_{minutes_since_midnight}_{mission_name}.log"


    print(f"export DAS_RUN_UUID={das_run_uuid}")
    print(f"export MISSION_NAME={mission_name}")
    print(f"export LOGFILE_NAME={LOGFILE_NAME}")

    obj[CC.START] = str(int(time.time()))
    das_log(f"{args.mission_type} Mission Start", extra=obj)

def mission_stop(obj, args):
    obj[CC.STOP] = str(int(time.time()))
    das_log(f"{args.mission_type} Mission Stop", extra=obj)


def config_upload(config):
    """Called from process_xml and from das2020_driver.py to upload config files. Requires a das_run_uuid in the global environment
    :param config: the configuration to upload. (Typically a HiearchicalConfigFile)
    """

    section_option_vals = []
    for section in config:
        for option in config[section]:
            section_option_vals.append((section, option, os.path.expandvars(config[section][option]) ))
    obj = {'configlist': json.dumps(section_option_vals)}
    send_obj(obj=obj)


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Sends messages to dashboard for the current mission, and control missions. Normally mission is specified by DAS_RUN_UUID environment variable." )
    parser.add_argument("--debug",         action='store_true', help='Print debug messages and set the debug flag in the obj that is sent.')
    parser.add_argument("--use_debug_url", action='store_true', help='Use the debug API endpoint')
    parser.add_argument("--exit_code", type=int, help="report exit_code for this mission")
    parser.add_argument("--log",                 help="send a log message",nargs='+')
    parser.add_argument("--tee",   action='store_true',              help="Log each line one line at a time, and send it to stdout")
    parser.add_argument("--runtest", action='store_true', help="start a new run and print the runID")
    parser.add_argument("--spark_submit", action='store_true', help='take spark-submit command from stdin')
    parser.add_argument("--verbose", action='store_true')
    parser.add_argument("--retry", action='store_true', help=f"Log a token_retry with retry count=1 and delay={FAKE_RETRY_VALUE} Used for debugging retry recording system")
    parser.add_argument('--mission_type', default='tda', help='mission type')
    parser.add_argument('--code', help='Specifies how messages should be logged. Should be '+str(CODE_LIST),default='CODE_ANNOTATE')
    parser.add_argument('--red', action='store_true', help='When running tee, print output in red')
    parser.add_argument('--write', help='--tee also writes to this file')

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--mission",      help="prints the mission name from the environment or a randomly generated mission name",action='store_true')
    group.add_argument("--mission_name", help="specify mission name")
    group.add_argument("--mission_start", action='store_true', help="Start the mission. Meant to be run from within a $() in a bash script. Creates DAS_RUN_UUID environment variables. Optionally creates the MISSION_NAME environment variable if it doesn't already exist. Sends message to the dashboard")
    group.add_argument("--mission_stop",  action='store_true', help='Stop the mission')

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('--stdout', action='store_true', help='--tee writes to stdout (default)')
    group.add_argument('--stderr', action='store_true', help='--tee writes to stderr instead of stdout')
    group.add_argument("--errors", action='store_true', help="provide JSON error on STDIN message that is sent for the current mission to the dashboard")

    args = parser.parse_args()

    if args.stderr and not args.tee:
        parser.error("--stderr requires --tee")

    if args.write and not args.tee:
        parser.error("--write requires --tee")

    if args.stdout and not args.tee:
        parser.error("--stdout requires --tee")

    # the mesage we will send
    obj    = {'debug':args.debug,
              'verbose':args.verbose,
              'mission_type':args.mission_type}
    if args.use_debug_url:
        obj[USE_DEBUG_URL] = args.use_debug_url

    if args.mission:
        print(get_mission_name())
        exit(0)

    if args.mission_name:
        os.environ[CC.MISSION_NAME] = args.mission_name
        obj['mission_name'] = args.mission_name

    if args.mission_start:
        mission_start(obj, args)
        exit(0)

    if args.mission_stop:
        mission_stop(obj, args)
        exit(0)

    if args.log:
        das_log(" ".join(args.log), code=str_to_code(args.code), debug=args.debug, verbose=args.verbose)

    if args.runtest:
        das_log("this is the end of a test run", extra={'stop':'now()'})

    if args.tee:
        das_tee(args=args)

    if args.exit_code is not None:
        das_log(extra={'exit_code':args.exit_code})

    if args.spark_submit:
        spark_submit = sys.stdin.readline()
        das_log("calling spark_submit",extra={'spark_submit':spark_submit})

    if args.retry:
        print("Sending retry message ..")
        if CC.DAS_RUN_UUID not in os.environ:
            os.environ[CC.DAS_RUN_UUID] = fake_das_run_uuid()
        token_retry(retry=1, delay=FAKE_RETRY_VALUE, use_debug_url=args.use_debug_url, success=1, debug=args.debug)
        print("Sent")

    if args.errors:
        obj['errors'] = json.dumps(json.load(sys.stdin))
        send_obj(obj=obj)
