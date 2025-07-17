package flink.demo.transfer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 示例数据源
        DataStream<String> text = env.fromElements(
                "flink spark hadoop",
                "flink spark",
                "hadoop"
        );

        // 流式处理
        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split(" ")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                // 分组：按第一个字段（word）keyBy
                .keyBy(value -> value.f0)
                // 聚合：按 key 统计次数
                .sum(1);

        // 输出
        counts.print();

        env.execute("Flink keyBy demo");
    }
}
