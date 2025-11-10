package flink.demo.timer;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateTimerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 并行度 2
        /*
       nc -lk 9999
       a,item1
       a,item2
       d,item3
       g,item4
       a,item5
         */
        DataStream<Event> source = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] parts = line.split(",");
                    return new Event(parts[0], parts[1]);
                });

        source
                // 外层 key = userId
                .keyBy(event -> event.userId)
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();
    }

    // 事件类
    public static class Event {
        public String userId; // 外层 key
        public String item;   // 内层 key

        public Event() {}

        public Event(String userId, String item) {
            this.userId = userId;
            this.item = item;
        }
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {

        // MapState<内层key, value>
        private MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("itemCount", String.class, Integer.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新 MapState
            Integer count = mapState.get(value.item);
            if (count == null) {
                count = 0;
            }
            mapState.put(value.item, count + 1);

            // 注册一个 5 秒后的定时器（事件时间或处理时间都可以，这里用处理时间）
            long timerTs = ctx.timerService().currentProcessingTime() + 1000;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 当前外层 key
            String outerKey = ctx.getCurrentKey();
            // 当前 subtask ID
            int subtaskId = getRuntimeContext().getIndexOfThisSubtask();

            StringBuilder sb = new StringBuilder();
            sb.append("[Subtask ").append(subtaskId).append("] ");
            sb.append("Timer triggered for outerKey=").append(outerKey).append(" | MapState content: ");

            for (String innerKey : mapState.keys()) {
                sb.append(innerKey).append("->").append(mapState.get(innerKey)).append(", ");
            }

            out.collect(sb.toString());
        }
    }
}