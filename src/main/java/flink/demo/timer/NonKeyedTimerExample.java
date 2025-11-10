package flink.demo.timer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

public class NonKeyedTimerExample {
    // 定义侧输出流标签
    private static final OutputTag<String> SIDE_OUTPUT_TAG =
            new OutputTag<String>("side-output", TypeInformation.of(String.class)) {
            };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 方便观察输出

        // 每秒生成一条数据
        DataStreamSource<String> source = env.addSource(new org.apache.flink.streaming.api.functions.source.SourceFunction<String>() {
            private volatile boolean running = true;
            private int counter = 0;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    ctx.collect("event-" + counter++);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // 非 keyed 流，使用 ProcessingTimeService 注册周期性定时器
        SingleOutputStreamOperator<String> myTimerOp = source.transform("MyTimerOp", TypeInformation.of(String.class),
                new MyNonKeyedTimerOperator(SIDE_OUTPUT_TAG));

        MyPrintSink<String> stringMyPrintSink = new MyPrintSink<String>("sink");
        myTimerOp.addSink(stringMyPrintSink);
        myTimerOp.getSideOutput(SIDE_OUTPUT_TAG).addSink(stringMyPrintSink);
        env.execute("Non-Keyed Timer Example");
    }

    public static class MyNonKeyedTimerOperator
            extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        private long nextFlushTime;
        private final long interval = 5000;
        private final OutputTag<String> sideOutputTag;


        public MyNonKeyedTimerOperator(OutputTag<String> sideOutputTag) {
            this.sideOutputTag = sideOutputTag;
        }

        @Override
        public void open() throws Exception {
        }

        private void scheduleNextTimer(long timestamp) {
            getProcessingTimeService().registerTimer(timestamp, ts -> {
                        output.collect(new StreamRecord<>("[TIMER] " + ts));
                        scheduleNextTimer(ts + interval);
                    }
            );
        }

        @Override
        public void processElement(StreamRecord<String> element) {
            scheduleNextFlushTimer();
            output.collect(new StreamRecord<>("[DATA] " + element.getValue()));
            output.collect(sideOutputTag, new StreamRecord<>("[SIDE] " + element.getValue()));
        }

        private void scheduleNextFlushTimer() {
            long currentProcessingTime = getProcessingTimeService().getCurrentProcessingTime();
            if (nextFlushTime == 0 || nextFlushTime < currentProcessingTime) {
                nextFlushTime = currentProcessingTime + interval;
                getProcessingTimeService().registerTimer(nextFlushTime, this::onTimer);
            }
        }

        private void onTimer(long timestamp) {
            System.out.println("onTimer");
            nextFlushTime = 0;
            scheduleNextFlushTimer();
            output.collect(new StreamRecord<>("[OnTimer] " + timestamp));
            output.collect(sideOutputTag, new StreamRecord<>("[SIDE-OnTimer] " + timestamp));
        }
    }

    public static class MyPrintSink<T> implements SinkFunction<T> {
        private final String prefix;

        public MyPrintSink(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void invoke(T value, Context context) {
            System.out.println(prefix + ": " + value);
        }
    }

}