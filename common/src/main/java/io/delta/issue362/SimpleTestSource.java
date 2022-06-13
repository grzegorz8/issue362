package io.delta.issue362;

import java.util.Collections;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;

public class SimpleTestSource extends RichParallelSourceFunction<RowData> implements CheckpointedFunction {

    private final int numberOfRecords;
    private final int keyCount;
    private ListState<Integer> nextValueState;
    private int nextValue;
    private volatile boolean isCanceled;

    SimpleTestSource(int numberOfRecords, int keyCount) {
        this.numberOfRecords = numberOfRecords;
        this.keyCount = keyCount;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextValue = nextValueState.get().iterator().next();
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        sendRecordsUntil(numberOfRecords, ctx);
        idleForever();
    }

    private void sendRecordsUntil(int targetNumber, SourceContext<RowData> ctx) {
        while (!isCanceled && nextValue < targetNumber) {
            synchronized (ctx.getCheckpointLock()) {
                RowData row = TestRow.CONVERTER.toInternal(
                    TestRow.randomRow(nextValue % keyCount)
                );
                ctx.collect(row);
                nextValue++;
            }
        }
    }

    private void idleForever() throws InterruptedException {
        while (!isCanceled) {
            Thread.sleep(10L);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}
