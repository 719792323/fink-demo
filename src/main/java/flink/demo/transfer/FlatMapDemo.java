package flink.demo.transfer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 示例数据：多行句子
        List<String> sentences = Arrays.asList("flink is cool", "java is powerful");
        // 数据源
        DataStream<String> source = env.fromCollection(sentences);
        // 使用 flatMap 拆分成单词
        DataStream<String> words = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                // 按空格分割并输出
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });
        //lambda
//        source.flatMap((String value, Collector<String> out) -> {
//            for (String word : value.split(" ")) {
//                out.collect(word);
//            }
//        }).returns(String.class);
        // 打印输出
        words.print();
        // 执行程序
        env.execute("Flink flatMap Demo");
    }
}
