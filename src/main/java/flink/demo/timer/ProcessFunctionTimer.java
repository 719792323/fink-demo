package flink.demo.timer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        source.process(new TestProcessFunction()).print();
        env.execute();
    }

    static class TestProcessFunction extends ProcessFunction<String, String> {
        private final long flushInterval = 1000;
        private long nextFlushTime;

        @Override
        public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
            if (nextFlushTime == 0) {
                scheduleNextFlushTimer(context);
            }
            collector.collect(s);
        }

        private void scheduleNextFlushTimer(ProcessFunction<String, String>.Context context) {
            if (nextFlushTime == 0 || nextFlushTime < context.timerService().currentProcessingTime()) {
                nextFlushTime = context.timerService().currentProcessingTime() + flushInterval;
                context.timerService().registerProcessingTimeTimer(nextFlushTime);
            }
        }

        @Override
        public void onTimer(long timestamp, ProcessFunction<String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("onTimer");
            scheduleNextFlushTimer(ctx);
        }
    }
}
