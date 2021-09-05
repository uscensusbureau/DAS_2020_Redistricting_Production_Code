# Logging in the DAS

Logging serves the following purposes within the DAS:

* Informs the developer of the algorithm's progress.
* Allows the developer to diagnose error conditions.
* Allows the developer to debug new features
* Allows analysis of algorithm's _machine performance_ (statistical performance is determined by analyzing the resulting data)
* Allows capture of intermediate values for algorithmic developing.

## Logging sources
Logging can happen in several places:

1. From the `run_cluster.sh` bash script that runs the `spark-submit` job.
2. From Java on the Master node.
3. From Python on the Master node.
4. From Java on the worker nodes.
4. From Python on the worker nodes.


Logging in bash scripts is done with the `echo` statement. These messages go to stdout. They can also be configured to go to stderr.

## Logging destinations
1. stdout from the `run_cluster.sh` script. This is ephemeral, although some people pipe the output of `run_cluster.sh` into a file. This is not necessary.
2. The mission log file. This is a file that is in the logs/ directory with the mission time and name, ending `.log`.
3. The mission dfxml file. This is also a file in the logs/ directory with the mission time and name, ending `.dfxml`. It contains structured data that is designed to be quickly findable in post-processing.
3. The Yarn log file. This file contains all of the Log4j output of Yarn on the Master and worker nodes, and all of the stdout output from the worker nodes. This is captured by the `run_cluster.sh` script in the log/s directory with the mission time and name, ending `yarnlog.txt.gz` (it's compressed). The first 10,000 lines are stored in the `_yarnlog.txt.head.txt` file and the last 10,000 lines are in `_yarnlog.txt.tail.txt`. 
4. LOCAL1 is a syslog facility. When the DAS starts up the Python `logging` system is set up so that `logging.info` calls on both the Master and worker nodes gets sent to LOCAL1 syslog on the Master node. During the execution of the DAS run the `/var/log/local1.log` file on the Master node is scanned for JSON dictionaries that include the current Yarn `applicationId`. These are all thrown into a list which is then used for the graphics that are stored in the`.html` log file. The Pandas dataframe is also written out as the `.df.csv` file and the `.h5` file.
5. Critical points in the operation of the DAS can be sent to the dashboard as an annotation. This is done by calling `das.annotate(message)`. The points are printed on STDOUT with 'ANNOTATE: <message>'. The messages annotations are also stored in both the `.log` and the `.dfxml` file and they are sent to the Dashboard by calling `programs.dashboard.das_log(message).` The messages are stored in the SQL table `das_log` on the dashboard and are displayed on the dashboard. Soon they will be annotated on the performance graphs.

All of these logging destinations are combined into a single `.zip` file and sent to AWS S3. These files are downloaded by the dashboard and unzipped in to the [https://dasexperimental.ite.ti.census.gov/das/runs/2019/](https://dasexperimental.ite.ti.census.gov/das/runs/2019/) directory.

## Log Mechanisms

Here are approaches for writing log messages:

1. You can include a `print` statement in your code. This is the worst way to log. The print statements just go to stdout and are lost.
2. You can call `das.log_and_print()` or `module.log_and_print()`. This sends output to the .log file and to stdout.
3. You can call `das.annotate()` for time-stampped messages that you want to appear on the dashboard and graphs.
4. You can call `logging.info()` for messages that should go to syslog. This is primarily messages from the mappers which are then combined to create the performance statistics. Right now the main entrance point for this is in `programs/nodes/nodes.py`.


# Moving forward
The current logging system works but it is not clean. Options for moving forward include:

1. Doing everything with das.annotate() overloading it
2. Doing everything with the Python-provided logging system and overloading it
3. Doing all logging with some new logging system.
4. Just leaving things as they are for now.

# Misc (this section requires cleanup)
Additional logging mechanisms/locations exist that are not part of the DAS proper, but are part of the infrastructure/context in which it operates (yarn, spark, EMR, etc). AWS GovCloud support staff, for example, have indicated interest in:

1. Per-container, per run "EMR Service Logs": stdout and stderr files located at locations like s3://uscb-decennial-ite-das-logs/j-14XYX41RIIVN/containers/application_1564059690213_1241/. These seem to primarily contain information on each executor's JVM garbage-collection frequency/volume, and on various low-level spark operations (e.g., specific subset of bytes read from a target input file). There is one file per container (and one container per executor, in yarn), so there can be many files, but each individual file tends to be modest in length (500-3000 lines). AWS support staff specifically asked about these logs in connection with our spark jobs that failed without throwing an explicit error message in the driver program's STDOUT, in late August 2019 (these jobs suddenly 'skipped' from one stage in the spark job to the very end, with no apparent reason for doing so)

2. CloudMetrics logs on network usage: "These logs should be able to be seen within the /<CLUSTER_ID>/node/<INSTANCE_ID_OF_NODE>/daemons/instance-state for the Lost Node and /<CLUSTER_ID>/node/<INSTANCE_ID_OF_NODE>/daemons/ for the Master Node." Examples of their names: Lost Node = i-0a364b97b2b7d7bc8: instance-state.log-2019-10-01-07-15.gz, instance-state.log-2019-10-01-07-30.gz, Master Node = i-0070462f2fa2e7061, instance-controller.log.2019-10-01-07.gz, instance-controller.log.2019-10-01-08.gz. AWS used these logs to conclude that a lost core node had been lost due to a large surge in network traffic, which caused the core node to be unresponsive for too long a period of time, and to be killed by the relevant yarn/spark watchdog process.

In addition, we have had several runs (the same runs noted in item one above) that have failed without successfully aggregating yarn logs. For these runs, Simson identified lengthy, json-based partial logs available at locations like: "/mnt/tmp/logs/{applicationId}{inProgressFlag} where inProgressFlag='.inprogress' if the job is still running." (quoting Simson's original description)

