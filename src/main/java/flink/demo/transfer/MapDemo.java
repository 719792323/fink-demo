package flink.demo.transfer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;
import java.util.List;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 示例数据源
        List<String> words = Arrays.asList("flink", "java", "stream");

        // 创建数据流
        DataStream<String> source = env.fromCollection(words);

        // 使用 map 算子将单词转为大写
        DataStream<String> upperCaseStream = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });
        //Lambda 写法
//        DataStream<String> upperCaseStream = source.map(s -> s.toUpperCase());
        //方法引用写法
//        DataStream<String> upperCaseStream = source.map(String::toUpperCase);
        // 打印结果
        upperCaseStream.print();
        // 执行程序
        env.execute("Flink Map Function Demo");
    }
}
