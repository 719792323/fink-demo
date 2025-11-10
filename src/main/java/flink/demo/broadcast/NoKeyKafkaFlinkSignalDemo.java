package flink.demo.broadcast;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/*
1. 创建topic
kafka-topics.sh --create --topic data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic signal-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

2. 生产数据
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic data-topic
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic signal-topic
 */
public class NoKeyKafkaFlinkSignalDemo {

    private static final MapStateDescriptor<String, String> signalDescriptor =
            new MapStateDescriptor<>(
                    "signal-state",
                    TypeInformation.of(String.class),
                    TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-signal-demo");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 主数据流
        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("data-topic", new SimpleStringSchema(), kafkaProps))
                .name("data-stream")
                .setParallelism(2); // 主流可以高并发

        // 信号流（单并行度）
        // 信号流必须设置并行度为1，否则会出现下游的task接收信号不全的问题
        DataStream<String> signalStream = env
                .addSource(new FlinkKafkaConsumer<>("signal-topic", new SimpleStringSchema(), kafkaProps))
                .name("signal-stream")
                .setParallelism(1);

        BroadcastStream<String> broadcastSignal = signalStream.broadcast(signalDescriptor);

        dataStream
                .connect(broadcastSignal)
                .process(new BroadcastProcessFunction<String, String, String>() {

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(signalDescriptor);
                        String signal = state.get("signal");
                        if ("REFRESH".equalsIgnoreCase(signal)) {
                            out.collect("⚡ 收到 REFRESH 信号, 触发刷新逻辑: " + value);
                        } else if ("PAUSE".equalsIgnoreCase(signal)) {
                            out.collect("⏸ 暂停处理: " + value);
                        } else {
                            out.collect("✅ 正常处理: " + value + " (signal=" + signal + ")");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String signal, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> state = ctx.getBroadcastState(signalDescriptor);
                        state.put("signal", signal);
                        out.collect("📢 信号更新为: " + signal);
                    }
                })
                .print();

        env.execute("Flink Signal Socket Demo");
    }
}
