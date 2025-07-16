package flink.demo.demo1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;
import java.util.List;

//Fink启动Demo
public class FlinkSimpleDemo {
    public static void main(String[] args) throws Exception {
        // 初始化执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建本地数据源
        List<String> input = Arrays.asList("hello", "flink", "java");
        DataStreamSource<String> source = env.fromCollection(input);

        // 转换逻辑：转为大写
        DataStream<String> upperCaseStream = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });

        // 输出到控制台
        upperCaseStream.print();

        // 执行任务
        env.execute("Flink Java Simple Demo");
    }
}
