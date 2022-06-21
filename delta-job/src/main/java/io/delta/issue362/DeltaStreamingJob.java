package io.delta.issue362;

import java.time.Duration;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

public class DeltaStreamingJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        int inputRecordsCount = parameters.getInt("input-records", 20_000_000);
        int partitionCount = parameters.getInt("partition-count", 2);
        int checkpointIntervalInSeconds = parameters.getInt("checkpoint-interval-in-seconds", 5);
        String tablePath = parameters.get("delta-table-path", "/opt/delta-test-data/");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(
            Duration.ofSeconds(checkpointIntervalInSeconds).toMillis(),
            CheckpointingMode.EXACTLY_ONCE
        );
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.disableOperatorChaining();

        env.addSource(new SimpleTestSource(inputRecordsCount, partitionCount))
            .sinkTo(createDeltaSink(tablePath));

        env.execute("Issue 362 - Delta");
    }

    private static DeltaSink<RowData> createDeltaSink(String deltaTablePath) {
        return DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                new Configuration(),
                TestRow.TEST_ROW_TYPE
            )
            .withPartitionColumns("id")
            .build();
    }
}
