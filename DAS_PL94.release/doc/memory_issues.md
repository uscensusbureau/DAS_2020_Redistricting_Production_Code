# Out of memory

We've seen two sets of memory issues relating to memory:

1.  If the MASTER memory is _too large_, we get memory errors. We
think that this is because the smaller memory size forces garbage collection to happen more often.

2. If the TASK memory is _too small_, we get memory errors in a variety of data frame operations.

## Ideas to try:

1. We can manually force the Java garbage collector to run with:

    spark.sparkContext._jvm.System.gc()

2. We can use the G1GC garbage collector with these JVM options:
```
-XX:+UseG1GC 
-XX:ConcGCThread=<XX> //Replace <XX> with appropriate configuration
```
Also, the following options might come in handy to look into GC details while fine-tuning:

```
-XX:+PrintFlagsFinal 
-XX:+PrintReferenceGC 
-verbose:gc 
-XX:+PrintGCDetails 
-XX:+PrintGCTimeStamps 
-XX:+PrintAdaptiveSizePolicy
-XX:+UnlockDiagnosticVMOptions 
-XX:+G1SummarizeConcMark  
```

## References
* https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html

* https://spark.apache.org/docs/latest/tuning.html#garbage-collection-tuning
"Our experience suggests that the effect of GC tuning depends on your application and the amount of memory available. There are (many more)[https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/index.html] tuning options described online, but at a high level, managing how frequently full GC takes place can help in reducing the overhead."

* https://dzone.com/articles/how-tune-java-garbage

## References Simson has processed:

* https://stackoverflow.com/questions/33689536/manually-calling-sparks-garbage-collection-from-pyspark

* https://stackoverflow.com/questions/34589051/garbage-collection-time-very-high-in-spark-application-causing-program-halt/34590161

