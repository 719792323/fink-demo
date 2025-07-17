# 算子

## 源算子

| 分类          | 示例                                       | 并行性 | 场景                   |
| ------------- | ------------------------------------------ | ------ | ---------------------- |
| 内置          | `fromCollection`                           | ❌      | 快速测试               |
| Socket        | `socketTextStream`                         | ❌      | 本地测试流             |
| 文件          | `readTextFile` / `FileSource`              | ✅      | 读取离线/在线文件      |
| Kafka         | `FlinkKafkaConsumer`, `KafkaSource`        | ✅      | 实时日志 / 消息处理    |
| 自定义        | `SourceFunction`, `ParallelSourceFunction` | ✅      | 特定业务系统接口       |
| Flink CDC     | `MySQLSource`, `PostgresSource`            | ✅      | 数据库变更捕捉（推荐） |
| DataGenerator | `DataGeneratorSource`                      | ✅      | 测试数据生成           |

## 转换算子

| 类型       | 算子                               | 说明                              |
| ---------- | ---------------------------------- | --------------------------------- |
| 元素级转换 | `map`                              | 一对一转换                        |
|            | `flatMap`                          | 一对多转换                        |
|            | `filter`                           | 元素筛选                          |
| 聚合       | `keyBy`                            | 按 key 分组                       |
|            | `reduce`                           | 分组后聚合                        |
|            | `sum`, `min`, `max`                | 分组后字段聚合                    |
|            | `aggregate`                        | 自定义聚合                        |
|            | `window`                           | 时间或计数窗口                    |
|            | `window + reduce/sum/...`          | 滑动、滚动、会话窗口处理          |
| 多流操作   | `connect`                          | 合并两个不同类型的流              |
|            | `union`                            | 合并多个相同类型的流              |
|            | `split` / `select`（旧 API）       | 按条件分流（推荐用 `sideOutput`） |
|            | `sideOutput`                       | 侧输出流                          |
| 富函数     | `map`, `flatMap` 等 + RichFunction | 可访问生命周期方法，如 open/close |
| 低层操作   | `process`                          | 更强控制，如状态访问、定时器注册  |

### map

```java
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
6> FLINK
7> JAVA
8> STREAM
```

### flatmap

```java
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
5> java
5> is
5> powerful
4> flink
4> is
4> cool
```

### filter

只有过滤条件返回true的时候才会被输出下一阶段

```java
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
10> flink
3> data
4> tool
12> great
```

### keyBy

keyBy主要是要输入分区的方法，且分区完之后要接函数，单独使用没什么意义

```java
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
1> (spark,1)
1> (spark,2)
11> (hadoop,1)
11> (hadoop,2)
10> (flink,1)
10> (flink,2)
```

####  一、**聚合类操作**

| 函数            | 说明                                                         |
| --------------- | ------------------------------------------------------------ |
| `.sum(field)`   | 对某字段求和                                                 |
| `.min(field)`   | 对某字段取最小值                                             |
| `.max(field)`   | 对某字段取最大值                                             |
| `.minBy(field)` | 返回字段最小值对应的整条记录                                 |
| `.maxBy(field)` | 返回字段最大值对应的整条记录                                 |
| `.reduce(func)` | 自定义聚合逻辑                                               |
| `aggregate`     | `aggregate` 是 Flink 中 `KeyedStream` 或 `WindowedStream` 上的**聚合算子**，相比 `reduce` 更加灵活，**输入类型和输出类型可以不同**，适合做计数、平均值等更复杂的聚合逻辑。 |

##### 常用聚合例子

```
.keyBy(t -> t.f0).sum(1)
1> (spark,1)
1> (spark,2)
11> (hadoop,1)
11> (hadoop,2)
10> (flink,1)
10> (flink,2)
```

##### reduce聚合例子

语法格式：

```java
keyedStream.reduce(new ReduceFunction<T>() {
    @Override
    public T reduce(T value1, T value2) throws Exception {
        // 返回合并后的结果
    }
});
```

也可以使用 Java 8 Lambda 表达式：

```java
keyedStream.reduce((v1, v2) -> {...});
```

假设我们有一个数据流，元素为 `(String key, Integer value)`，我们希望按 key 对 value 求和。

```java
DataStream<Tuple2<String, Integer>> stream = env.fromElements(
    Tuple2.of("a", 1),
    Tuple2.of("b", 2),
    Tuple2.of("a", 3),
    Tuple2.of("b", 4)
);

stream
    .keyBy(t -> t.f0)  // 按字符串 key 分组
    .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))  // 聚合 value
    .print();
```

输出（顺序取决于执行环境）：

```
(a,1)
(b,2)
(a,4)
(b,6)
```

reduce 也可以配合窗口进行聚合，避免无限累加：

```java
stream
    .keyBy(t -> t.f0)
    .timeWindow(Time.seconds(10))  // 10秒滚动窗口
    .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
    .print();
```

##### aggregate

`aggregate` 是 Flink 中 `KeyedStream` 或 `WindowedStream` 上的**聚合算子**，相比 `reduce` 更加灵活，**输入类型和输出类型可以不同**，适合做计数、平均值等更复杂的聚合逻辑。

适用于 `WindowedStream`：

```java
WindowedStream<T, KEY, WINDOW> windowedStream = ...;
windowedStream.aggregate(new AggregateFunction<IN, ACC, OUT>());
```

- `IN`：输入类型（例如 `Tuple2<String, Integer>`）
- `ACC`：中间状态（累加器）的类型（例如 `Tuple2<Integer, Integer>` 表示累计值和次数）
- `OUT`：最终输出类型（例如 `Double`）

接口定义：

```java
public interface AggregateFunction<IN, ACC, OUT> {
    ACC createAccumulator();                            // 创建累加器
    ACC add(IN value, ACC accumulator);                 // 添加一个元素
    OUT getResult(ACC accumulator);                     // 返回最终结果
    ACC merge(ACC a, ACC b);                            // 合并两个累加器（用于 session/window 合并）
}
```

示例：统计每个 key 的平均值

输入数据：`Tuple2<String, Integer>`（如：("a", 3), ("a", 5), ("a", 7)）

目标：每个 key 10 秒内的平均值。

```java
public class AverageAggregate implements AggregateFunction<
        Tuple2<String, Integer>,         // IN
        Tuple2<Integer, Integer>,        // ACC: sum, count
        Double> {                        // OUT

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> acc) {
        return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> acc) {
        return acc.f1 == 0 ? 0.0 : (double) acc.f0 / acc.f1;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
```

使用方式：

```java
stream
    .keyBy(t -> t.f0)                               // 按 key 分组
    .timeWindow(Time.seconds(10))                   // 开窗
    .aggregate(new AverageAggregate())              // 自定义聚合函数
    .print();
```

适用场景：

- 平均值（value / count）
- 百分位估算（与 Apache DataSketches 一起使用）
- TopN 排序（可输出 List）
- 自定义复杂聚合（多个字段组合）

#### 二、**窗口类操作**

| 函数                                 | 说明                             |
| ------------------------------------ | -------------------------------- |
| `.window(WindowAssigner)`            | 定义窗口类型（滚动、滑动、会话） |
| `.timeWindow(Time size)`             | 快捷定义滚动窗口                 |
| `.timeWindow(Time size, Time slide)` | 快捷定义滑动窗口                 |
| `.countWindow(int size)`             | 按条数窗口                       |
| `.process(ProcessFunction)`          | 低级窗口处理逻辑                 |
| `.apply(WindowFunction)`             | 自定义窗口聚合                   |

```
.keyBy(t -> t.f0)
.timeWindow(Time.seconds(10))
.sum(1)
```

####  三、**状态类操作（高级处理）**

| 函数                             | 说明                                |
| -------------------------------- | ----------------------------------- |
| `.process(KeyedProcessFunction)` | 获取 key 状态、定时器等复杂逻辑处理 |
| `.flatMap()`                     | 可用于对每个 key 自定义发射记录     |
| `.map()`                         | 对 key 分区后的每条记录进行映射处理 |
| `.filter()`                      | 对 key 后的记录过滤                 |

```
.keyBy(t -> t.f0)
.process(new KeyedProcessFunction<...>() { ... })
```

##### process(KeyedProcessFunction)

`process(KeyedProcessFunction)` 是 Flink 中最强大的算子之一，适用于对 **`KeyedStream`** 进行**自定义处理**，可以：

- 自定义状态（State）
- 注册定时器（Timer）
- 精细控制每条数据的处理逻辑

```java
public abstract class KeyedProcessFunction<KEY, IN, OUT> 
        extends AbstractRichFunction 
        implements Function {

    // 处理每条输入数据
    public abstract void processElement(
        IN value,
        Context ctx,
        Collector<OUT> out) throws Exception;

    // 处理定时器触发
    public void onTimer(
        long timestamp,
        OnTimerContext ctx,
        Collector<OUT> out) throws Exception {}
}
```

**示例：10秒内连续出现两次同一个 key，就报警**

```java
public class KeyedAlertFunction extends KeyedProcessFunction<String, String, String> {

    // 状态：上一次到来的时间戳
    private transient ValueState<Long> lastTimeState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("last-time", Long.class);
        lastTimeState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        Long last = lastTimeState.value();
        long now = ctx.timestamp(); // 当前事件时间（注意：必须是事件时间语义）

        if (last != null && (now - last) < 10_000) {
            out.collect("10秒内重复出现 key: " + ctx.getCurrentKey());
        }

        // 更新时间
        lastTimeState.update(now);
    }
}

stream
    .assignTimestampsAndWatermarks(...)          // 必须设置时间戳和 watermark
    .keyBy(x -> x)                               // 按 key 分组
    .process(new KeyedAlertFunction())           // 调用自定义函数
    .print();
```

**示例：如果某个 key 5 秒内没有出现新的事件，则输出“超时”**

在 `KeyedProcessFunction` 中可以注册两种定时器：

| 时间类型     | 注册方法                                                    |
| ------------ | ----------------------------------------------------------- |
| **事件时间** | `ctx.timerService().registerEventTimeTimer(timestamp)`      |
| **处理时间** | `ctx.timerService().registerProcessingTimeTimer(timestamp)` |

取消定时器：

```java
ctx.timerService().deleteEventTimeTimer(timestamp);
ctx.timerService().deleteProcessingTimeTimer(timestamp);
```

```java
public class InactiveUserFunction extends KeyedProcessFunction<String, String, String> {

    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("timer", Long.class);
        timerState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        long currentEventTime = ctx.timestamp(); // 获取当前事件的事件时间

        // 清除旧定时器（如果有）
        Long oldTimer = timerState.value();
        if (oldTimer != null) {
            ctx.timerService().deleteEventTimeTimer(oldTimer);
        }

        // 注册一个新的事件时间定时器：5秒后触发
        long newTimer = currentEventTime + 5000;
        ctx.timerService().registerEventTimeTimer(newTimer);
        timerState.update(newTimer);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("用户 " + ctx.getCurrentKey() + " 在 5 秒内未活跃，可能已离线！");
        timerState.clear(); // 清除状态
    }
}

stream
    .assignTimestampsAndWatermarks(...)  // 必须使用事件时间语义
    .keyBy(value -> value)
    .process(new InactiveUserFunction())
    .print();
```

#### 四、其他组合操作

- `.connect(...)`：连接两个 keyedStream
- `.join(...)`：与另一个流做 keyBy join（需要窗口）
- `.coGroup(...)`：流对流 group join

## 时间窗口

### 时间维度

| 维度     | 说明               | 示例                                               |
| -------- | ------------------ | -------------------------------------------------- |
| 时间语义 | 窗口如何度量“时间” | 事件时间（EventTime）、处理时间（ProcessingTime）  |
| 窗口类型 | 数据如何被分批     | 滚动窗口、滑动窗口、会话窗口、全局窗口、自定义窗口 |

### 窗口类型

| 窗口类型             | 说明                       | 方法                                                         | 特点                     |
| -------------------- | -------------------------- | ------------------------------------------------------------ | ------------------------ |
| 滚动窗口（Tumbling） | 固定长度、不重叠           | `.timeWindow(Time.seconds(10))`                              | 每 10s 触发一次          |
| 滑动窗口（Sliding）  | 固定长度、可重叠           | `.timeWindow(Time.seconds(10), Time.seconds(5))`             | 每 5s 滑动，窗口大小 10s |
| 会话窗口（Session）  | 基于不活跃时间划分窗口     | `.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))` | 动态长度                 |
| 全局窗口（Global）   | 需自定义触发器才生效       | `.window(GlobalWindows.create())`                            | 不常用                   |
| 自定义窗口           | 实现 `WindowAssigner` 接口 | `.window(MyWindowAssigner)`                                  | 灵活但复杂               |

### 窗口API构成

```java
stream
    .keyBy(...)                        // 分组
    .window(...)                       // 窗口分配器
    .trigger(...)                      // 触发器（可选）
    .evictor(...)                      // 驱逐器（可选）
    .allowedLateness(...)              // 允许迟到（可选）
    .sideOutputLateData(...)           // 迟到数据侧输出（可选）
    .reduce() / aggregate() / apply()  // 聚合操作
```

#### trigger

你可以把：

- **窗口（Window）** 理解成一个“容器”或“收集盒子”，它根据时间或数据来划分范围，决定“哪些数据是一起处理的”；
- **触发器（Trigger）** 是“处理信号器”，它决定“啥时候处理这个盒子里的数据”。

> **窗口定义了 “范围”**，**触发器定义了 “时机”**。

假设你有如下的数据流（每个数字是一条数据，带有事件时间）：

```
lua复制编辑数据流：1----2----3----4----5----6----...
时间： 0s   2s   4s   6s   8s  10s
```

你设置了一个 **5秒的滚动窗口**：

```java
.window(TumblingEventTimeWindows.of(Time.seconds(5)))
```

| 窗口范围   | 包含数据 |
| ---------- | -------- |
| [0s, 5s)   | 1, 2, 3  |
| [5s, 10s)  | 4, 5     |
| [10s, 15s) | 6, ...   |

然后你配置一个 `EventTimeTrigger`：

Flink 会在 Watermark ≥ 窗口结束时间时，**触发一次计算**，对这个窗口里的数据做处理（如 sum、reduce、aggregate 等）。

```java
stream
    .keyBy(x -> x.userId)
    .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 定义窗口范围
    .trigger(CountTrigger.of(5)) // 定义触发时机（每5条数据触发一次）
    .reduce(new MyReduceFunction()); // 定义计算逻辑
```

含义是：

- 每个 key 会有一个 10s 的时间窗口；
- 每个窗口中每收集到 5 条数据就触发一次计算；
- ==注意：**不一定要等到窗口结束才能计算**，这个就是 trigger 发挥作用的地方。==

控制 **窗口何时触发计算**，比如：

- 收到多少条数据后触发
- 到达某个时间点触发
- 周期性触发（也可以用）

| 触发器名                                         | 功能描述                        |
| ------------------------------------------------ | ------------------------------- |
| `EventTimeTrigger`                               | 基于事件时间触发（默认）        |
| `ProcessingTimeTrigger`                          | 基于处理时间触发                |
| `CountTrigger.of(n)`                             | 每收到 n 条数据就触发一次       |
| `ContinuousProcessingTimeTrigger.of(Time.of(x))` | 每隔 x 时间处理一次（处理时间） |
| `ContinuousEventTimeTrigger.of(Time.of(x))`      | 每隔 x 时间处理一次（事件时间） |

示例：CountTrigger 每3条数据触发一次

```java
stream
    .keyBy(r -> r.id)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .trigger(CountTrigger.of(3)) // 每3条触发一次
    .reduce(new MyReduceFunction());
```

自定义 Trigger 示例：每来一条数据立即触发，并清空窗口（仿佛没有窗口）

```java
public class EveryElementTrigger<T> extends Trigger<T, TimeWindow> {

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE; // 触发并清空窗口
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE; // 处理时间不触发
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE; // 事件时间不触发
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // 可以清理定时器等
    }
}
stream
    .keyBy(r -> r.id)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .trigger(new EveryElementTrigger<>())
    .reduce(new MyReduceFunction());
```

| 返回值           | 含义                         |
| ---------------- | ---------------------------- |
| `FIRE`           | 触发窗口计算，但保留窗口内容 |
| `FIRE_AND_PURGE` | 触发并清空窗口               |
| `CONTINUE`       | 什么也不做                   |
| `PURGE`          | 不触发，但清空窗口内容       |

#### evictor

#### allowedLateness

#### sideOutputLateData

#### apply

### 水位线



## 输出算子

