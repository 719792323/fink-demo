package state;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.UUID;

public class ValueStateTest {

    // 自定义对象，必须序列化
    public static class MyObject {
        public int value;

        public MyObject() {
        }

        public MyObject(int value) {
            this.value = value;
        }
    }

    static class TestKeyProcessFunction extends KeyedProcessFunction<String, String, String> {
        private ValueState<MyObject> state;
        private transient Cache<String, String> lruCache;

        static {
            // 加载本地库
            RocksDB.loadLibrary();
        }

        transient RocksDB db = null;

        TestKeyProcessFunction() {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<MyObject> descriptor =
                    new ValueStateDescriptor<>(
                            "myState",
                            TypeInformation.of(MyObject.class));
            state = getRuntimeContext().getState(descriptor);
            Options options = new Options();
            options.setCreateIfMissing(true); // 如果数据库不存在就创建
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setBlockCache(new LRUCache(128 * 1024 * 1024));//128M
            options.setTableFormatConfig(tableConfig);
            File baseDBPath = new File("/tmp/testrocksdb/");
            if (!baseDBPath.exists()) {
                baseDBPath.mkdirs();
            }
            db = RocksDB.open(options, "/tmp/testrocksdb/" + UUID.randomUUID() + "/");
            byte[] key = "myKey".getBytes();
            byte[] value = "Hello RocksDB".getBytes();
            db.put(key, value);

            lruCache = Caffeine.newBuilder()
                    .maximumSize(10).build();
            lruCache.put("key1", "hello");
        }

        @Override
        public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
            MyObject obj1 = state.value();   // 第一次读（初始为null）
            if (obj1 == null) {
                obj1 = new MyObject(42);
                state.update(obj1);
            }

            MyObject obj2 = state.value();   // 第二次读

            boolean sameObject = (obj1 == obj2);

            collector.collect("obj1==obj2 ? " + sameObject);
            byte[] getValue = db.get("myKey".getBytes());
            if (getValue != null) {
                System.out.println("读取数据: " + new String(getValue));
            } else {
                System.out.println("Key不存在");
            }
            System.out.println(lruCache.getIfPresent("key1"));
        }

        @Override
        public void close() throws Exception {
            if (db != null) {
                db.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("key1")
                .keyBy(s -> s)
                .process(new TestKeyProcessFunction())
                .print();
        env.execute();
    }

}
