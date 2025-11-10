package flink.demo.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/*
nc -lk 9999
nc -lk 9998
 */
public class NoKeyFlinkSignalDemo {

    private static final MapStateDescriptor<String, String> signalDescriptor =
            new MapStateDescriptor<>(
                    "signal-state",
                    TypeInformation.of(String.class),
                    TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 主数据流
        DataStream<String> dataStream = env.socketTextStream("localhost", 9998);

        // 信号流来
        DataStream<String> signalStream = env.socketTextStream("localhost", 9999);

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
