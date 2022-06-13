package io.delta.issue362;

import java.time.Duration;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

public class ParquetStreamingJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        int inputRecordsCount = parameters.getInt("input-records", 2_000_000);
        int partitionCount = parameters.getInt("partition-count", 10);
        int checkpointIntervalInSeconds = parameters.getInt("checkpoint-interval-in-seconds", 60);
        String outputPath = parameters.get("output-path", "/opt/parquet-test-data/");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(
            Duration.ofSeconds(checkpointIntervalInSeconds).toMillis(),
            CheckpointingMode.EXACTLY_ONCE
        );
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.disableOperatorChaining();

        env.addSource(new SimpleTestSource(inputRecordsCount, partitionCount))
            .sinkTo(createParquetSink(outputPath));

        env.execute("Issue 362 - Parquet");
    }

    private static FileSink<RowData> createParquetSink(String deltaTablePath) {
        return FileSink.forBulkFormat(
                new Path(deltaTablePath),
                ParquetRowDataBuilder.createWriterFactory(
                    TestRow.TEST_ROW_TYPE,
                    new Configuration(),
                    true
                )
            )
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .withBucketAssigner(new TestBucketAssigner())
            .build();
    }

    private static class TestBucketAssigner implements BucketAssigner<RowData, String> {
        @Override
        public String getBucketId(RowData rowData, BucketAssigner.Context context) {
            return String.format("id=%s", rowData.getInt(0));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }


}
