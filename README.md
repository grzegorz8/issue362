# Delta Flink Connector - Issue #362

This repo recreates [Issue #362](https://github.com/delta-io/connectors/issues/362).

## Docker image

```bash
docker-compose build
docker-compose up
```

* Please note that two volumes are mounted:
  ```
    volumes:
      - /tmp/delta-test-data/:/opt/delta-test-data/
      - /tmp/parquet-test-data/:/opt/parquet-test-data/
  ```
* `HeapDumpOnOutOfMemoryError` is enabled.
* Task Manager's port 5000 is enabled for debugging.
* Debug log level is enabled for two packages:
  ```
  logger.parquet.name = org.apache.parquet.hadoop
  logger.parquet.level = DEBUG
  logger.delta.name = io.delta
  logger.delta.level = DEBUG
  ```

## How to recreate OutOfMemoryError

OOM error is thrown when the number of partitions is big enough (e.g 10). A similar behaviour is observed not only for
Delta connector but also for a plain Flink's FileSink writing Parquet files. 

1. Start docker environment.
2. Build artifacts.
   ```
   mvn clean install
   ```
3. Go to Flink UI: `http://localhost:8081`.
4. Submit a new job by adding a jar via UI. (`Submit New Job` > Add new)
5. Start `io.delta.issue362.DeltaStreamingJob` or `io.delta.issue362.ParquetStreamingJob`.
   Parallelism: `1`, Program Arguments: `--partition-count 10`.
6. Job should fail within a minute or two.

## How to recreate InvalidRecordException

1. Start docker environment.
2. Build artifacts.
   ```
   mvn clean install
   ```
3. Go to Flink UI: `http://localhost:8081`.
4. Submit a new job by adding a jar via UI. (`Submit New Job` > Add new)
5. Start `io.delta.issue362.DeltaStreamingJob`.
   Parallelism: `2`,
   Program Arguments: `--partition-count 2 --checkpoint-interval-in-seconds 10 --input-records 10000000`.
6. Job should fail within two minutes.

The following error is thrown when Delta Flink Connector tries to create a checkpoint.

```
grzegorz@grzegorz /tmp/delta-test-data/_delta_log $ ls -laht
total 72K
-rw-r--r-- 1     9999     9999  12K cze 13 09:26 .00000000000000000010.checkpoint.parquet.6b33fcd4-2b7a-4404-ba20-b230dee67f29.tmp
-rw-r--r-- 1     9999     9999  104 cze 13 09:26 ..00000000000000000010.checkpoint.parquet.6b33fcd4-2b7a-4404-ba20-b230dee67f29.tmp.crc
drwxr-xr-x 2     9999     9999 4,0K cze 13 09:26 .
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:26 00000000000000000010.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:26 00000000000000000009.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:26 00000000000000000008.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:26 00000000000000000007.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:26 00000000000000000006.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:25 00000000000000000005.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:25 00000000000000000004.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:25 00000000000000000003.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:25 00000000000000000002.json
-rw-r--r-- 1     9999     9999 1,3K cze 13 09:25 00000000000000000001.json
-rw-r--r-- 1     9999     9999 4,4K cze 13 09:25 00000000000000000000.json
drwxrwxrwx 5 grzegorz grzegorz 4,0K cze 13 09:25 ..
```

```
Caused by: org.apache.parquet.io.InvalidRecordException: map not found in optional group partitionValues (MAP) {
  repeated group key_value {
    required binary key (STRING);
    optional binary value (STRING);
  }
}
	at org.apache.parquet.schema.GroupType.getFieldIndex(GroupType.java:175)
	at org.apache.parquet.schema.GroupType.getType(GroupType.java:207)
	at com.github.mjakubowski84.parquet4s.MapParquetRecord.write(ParquetRecord.scala:460)
	at com.github.mjakubowski84.parquet4s.RowParquetRecord.$anonfun$write$1(ParquetRecord.scala:173)
	at com.github.mjakubowski84.parquet4s.RowParquetRecord.$anonfun$write$1$adapted(ParquetRecord.scala:167)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:58)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:51)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
	at com.github.mjakubowski84.parquet4s.RowParquetRecord.write(ParquetRecord.scala:167)
	at com.github.mjakubowski84.parquet4s.ParquetWriteSupport.$anonfun$write$2(ParquetWriter.scala:175)
	at com.github.mjakubowski84.parquet4s.ParquetWriteSupport.$anonfun$write$2$adapted(ParquetWriter.scala:169)
	at scala.collection.Iterator.foreach(Iterator.scala:937)
	at scala.collection.Iterator.foreach$(Iterator.scala:937)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1425)
	at com.github.mjakubowski84.parquet4s.ParquetWriteSupport.write(ParquetWriter.scala:169)
	at com.github.mjakubowski84.parquet4s.ParquetWriteSupport.write(ParquetWriter.scala:162)
	at org.apache.parquet.hadoop.InternalParquetRecordWriter.write(InternalParquetRecordWriter.java:128)
	at org.apache.parquet.hadoop.ParquetWriter.write(ParquetWriter.java:301)
	at com.github.mjakubowski84.parquet4s.DefaultParquetWriter.$anonfun$write$1(ParquetWriter.scala:144)
	at com.github.mjakubowski84.parquet4s.DefaultParquetWriter.$anonfun$write$1$adapted(ParquetWriter.scala:143)
	at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:32)
	at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:29)
	at scala.collection.mutable.WrappedArray.foreach(WrappedArray.scala:37)
	at com.github.mjakubowski84.parquet4s.DefaultParquetWriter.write(ParquetWriter.scala:143)
	at com.github.mjakubowski84.parquet4s.DefaultParquetWriter.write(ParquetWriter.scala:149)
	at io.delta.standalone.internal.Checkpoints$.$anonfun$writeCheckpoint$4(Checkpoints.scala:256)
	at io.delta.standalone.internal.Checkpoints$.$anonfun$writeCheckpoint$4$adapted(Checkpoints.scala:255)
	at scala.collection.immutable.List.foreach(List.scala:388)
	at io.delta.standalone.internal.Checkpoints$.writeCheckpoint(Checkpoints.scala:255)
	at io.delta.standalone.internal.Checkpoints.checkpoint(Checkpoints.scala:125)
	at io.delta.standalone.internal.Checkpoints.checkpoint$(Checkpoints.scala:121)
	at io.delta.standalone.internal.DeltaLogImpl.checkpoint(DeltaLogImpl.scala:41)
	at io.delta.standalone.internal.OptimisticTransactionImpl.postCommit(OptimisticTransactionImpl.scala:383)
	at io.delta.standalone.internal.OptimisticTransactionImpl.commit(OptimisticTransactionImpl.scala:155)
	at io.delta.flink.sink.internal.committer.DeltaGlobalCommitter.doCommit(DeltaGlobalCommitter.java:319)
	at io.delta.flink.sink.internal.committer.DeltaGlobalCommitter.commit(DeltaGlobalCommitter.java:222)
	at org.apache.flink.streaming.runtime.operators.sink.StreamingGlobalCommitterOperator.commit(StreamingGlobalCommitterOperator.java:83)
	at org.apache.flink.streaming.runtime.operators.sink.AbstractStreamingCommitterOperator.commitUpTo(AbstractStreamingCommitterOperator.java:154)
	at org.apache.flink.streaming.runtime.operators.sink.AbstractStreamingCommitterOperator.notifyCheckpointComplete(AbstractStreamingCommitterOperator.java:136)
	at org.apache.flink.streaming.runtime.operators.sink.StreamingGlobalCommitterOperator.notifyCheckpointComplete(StreamingGlobalCommitterOperator.java:99)
	at org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper.notifyCheckpointComplete(StreamOperatorWrapper.java:99)
	at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.notifyCheckpointComplete(SubtaskCheckpointCoordinatorImpl.java:283)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.notifyCheckpointComplete(StreamTask.java:990)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointCompleteAsync$11(StreamTask.java:961)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointOperation$13(StreamTask.java:977)
	at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$1.runThrowing(StreamTaskActionExecutor.java:47)
	at org.apache.flink.streaming.runtime.tasks.mailbox.Mail.run(Mail.java:78)
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail(MailboxProcessor.java:302)
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:184)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:575)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:539)
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:722)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:547)
	at java.lang.Thread.run(Thread.java:748)
```
