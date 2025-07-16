package flink.demo.demo2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class FlinkOperatorsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 源算子：本地集合输入
        List<String> input = Arrays.asList("apple", "banana", "apple", "orange", "banana", "apple");
        DataStream<String> source = env.fromCollection(input);
        // 转换算子：map - 转为大写
        DataStream<String> mapped = source.map((MapFunction<String, String>) String::toUpperCase);
        // 转换算子：filter - 只保留以字母 A 开头的单词
        DataStream<String> filtered = mapped.filter((FilterFunction<String>) word -> word.startsWith("A"));
        // 转换算子：flatMap - 拆分字符串为 (word, 1) 对
        DataStream<Tuple2<String, Integer>> flatMapped = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String word, Collector<Tuple2<String, Integer>> out) {
                out.collect(new Tuple2<>(word, 1));
            }
        });
        // 分组 + 聚合算子：按 key 分组，统计每个单词出现次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = flatMapped
                .keyBy(tuple -> tuple.f0) // 按第一个字段（word）分组
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                });
        // 输出算子：打印各个阶段输出
        System.out.println("===== Mapped（大写）输出 =====");
        mapped.print();
        System.out.println("===== Filtered（以A开头）输出 =====");
        filtered.print();
        System.out.println("===== WordCount 输出 =====");
        wordCount.print();
        // 执行任务
        env.execute("Flink All Common Operators Demo");
    }
}
