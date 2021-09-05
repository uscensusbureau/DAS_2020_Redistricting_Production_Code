Failure log:

on:

DASEMRCluster-20190121               j-12LPNIOSJ5SHC
 Mon Jan 21 16:54:29 2019     23+ hours
    bootstrap: TI-BootStrap s3://uscb-decennial-ite-das-inf/emrSetupV2.sh
    bootstrap: DAS-BootStrap s3://uscb-decennial-ite-das/bootstrap-TI.sh
    CORE  :  3 x m4.16xlarge =  12.91/hour
    MASTER:  1 x m4.2xlarge  =   0.65/hour
    Master: ip-10-252-44-46.ite.ti.census.gov
    Total cost: $311.74   ($13.55/hour)

m4.2xlarge = 4 vCPU, 16GiB
m4.16xlarge = 64 vCPU, 256 GiB

19/01/22 15:23:57 ERROR TaskSetManager: Task 814 in stage 123.0 failed 4 times; aborting job
19/01/22 15:23:57 ERROR SparkHadoopWriter: Aborting job job_20190122150030_0142.

ri - runs
nat - causes failure with: 
SPARK_RESOURCES="--driver-memory 5g --num-executors 15 --executor-memory 16g --executor-cores 4 --driver-cores 10 --conf spark.dynamicAllocation.maxExecutors=25 --conf spark.driver.maxResultSize=0g --conf spark.executor.memoryOverhead=20g"

* That's 5 executors per CORE node, 80GiB memory used per core node.
* So we have 256-80 = 176GiB of RAM unused on each CORE node.

On the master, we have 10 cores allocated, but the computer only has 4 cores....

New configuration:
SPARK_RESOURCES="--driver-memory 5g --num-executors 12 --executor-memory 64g --executor-cores 16 --driver-cores 4 --conf spark.driver.maxResultSize=8g --conf spark.executor.memoryOverhead=2g"
19/01/22 16:02:01 ERROR YarnScheduler: Lost executor 5 on ip-10-252-47-154.ite.ti.census.gov: Container killed by YARN for exceeding memory limits. 66.4 GB of 66 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead.

Note: observe that 66g = 64g + 2g


Let's try:
SPARK_RESOURCES="--driver-memory 5g --num-executors 12 --executor-memory 64g --executor-cores 16 --driver-cores 4 --conf spark.driver.maxResultSize=8g --conf spark.executor.memoryOverhead=10g"
Note: Total memory will be (64+10)*3 = 222g

