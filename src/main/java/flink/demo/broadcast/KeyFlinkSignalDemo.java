package flink.demo.broadcast;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KeyFlinkSignalDemo {

    // 广播状态描述符
    private static final MapStateDescriptor<String, String> signalDescriptor =
            new MapStateDescriptor<>("signals", TypeInformation.of(String.class), TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {
        // 1️⃣ 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Kafka 参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-signal-demo");

        // 3️⃣ 主数据源 Kafka
        FlinkKafkaConsumer<String> dataConsumer = new FlinkKafkaConsumer<>(
                "data-topic", new SimpleStringSchema(), props);
        DataStream<String> dataStream = env.addSource(dataConsumer).name("data-stream");

        // 4️⃣ 信号源 Kafka
        FlinkKafkaConsumer<String> signalConsumer = new FlinkKafkaConsumer<>(
                "signal-topic", new SimpleStringSchema(), props);
        DataStream<String> signalStream = env.addSource(signalConsumer).name("signal-stream");

        // 5️⃣ 广播信号流
        BroadcastStream<String> broadcastSignal = signalStream.broadcast(signalDescriptor);

        // 6️⃣ 连接并处理
        dataStream
                .keyBy(value -> 0) // 简单的单key分区
                .connect(broadcastSignal)
                .process(new KeyedBroadcastProcessFunction<Integer, String, String, String>() {

                    private transient ValueState<String> lastSignalState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> desc = new ValueStateDescriptor<>("last-signal", String.class);
                        lastSignalState = getRuntimeContext().getState(desc);
                    }

                    // 处理主流数据
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> signalState = ctx.getBroadcastState(signalDescriptor);
                        String currentSignal = signalState.get("signal");
                        if (currentSignal != null && currentSignal.equals("REFRESH")) {
                            out.collect("⚡收到信号 REFRESH，刷新逻辑触发: " + value);
                        } else {
                            out.collect("正常数据处理: " + value);
                        }
                    }

                    // 处理信号流数据
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> state = ctx.getBroadcastState(signalDescriptor);
                        state.put("signal", value);
                        out.collect("📢广播信号更新为: " + value);
                    }
                })
                .print();

        // 7️⃣ 启动作业
        env.execute("Flink Signal Broadcast Demo");
    }
}
