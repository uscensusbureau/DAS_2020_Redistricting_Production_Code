2019-12-30
- FORESEEABLE_SMELL - failed with:
```
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 16573 in stage 197.0 failed 8 times, most recent failure: Lost task 16573.7 in stage 197.0 (TID 224096, ip-10-252-45-45.ite.ti.census.gov, executor 1867): ExecutorLostFailure (executor 1867 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits.  36.1 GB of 36 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead or disabling yarn.nodemanager.vmem-check-enabled because of YARN-4714.
```
even though I had:
```
    --conf yarn.nodemanager.vmem-check-enabled=false
```
