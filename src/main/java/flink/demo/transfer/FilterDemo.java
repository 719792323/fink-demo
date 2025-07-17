package flink.demo.transfer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;
import java.util.List;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 示例数据：单词列表
        List<String> words = Arrays.asList("flink", "is", "great", "a", "big", "data", "tool");

        // 数据源
        DataStream<String> source = env.fromCollection(words);

        // 过滤长度大于 3 的单词
        DataStream<String> filtered = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return value.length() > 3;
            }
        });

        // 打印结果
        filtered.print();

        // 执行程序
        env.execute("Flink Filter Demo");
    }
}
