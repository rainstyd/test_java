package flink.datastream;

import flink.utils.GetConfigFromFile;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.redis.RedisSink;


public class GetKafkaToRedis {
    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.datastream.GetKafkaToMysql rainsty/test_flink-1.0-SNAPSHOT.jar --config=rainsty/test_flink.properties
        /* kafka source data (start line do not have Space)
            正常:<13:12:32.303>45673BD6BC4[1]
            stockcode=1
            b=2
            ----------异常:<13:12:32.303>45673BD6BA3[0]
            stockcode=3
            d=4
        */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("bootstrap.servers", "ERROR: 读取配置文件异常！"));

        String redisKey = properties.getProperty("redis.key");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics(properties.getProperty("topic.name.src"))
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<Tuple2<String, Integer>> dataStream = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) { // 解析kafka日志内容
                try {
                    String[] parts = value.split("----------");

                    for (String part : parts) { // 后续可调整成正则
                        String[] keyValuePairs = part.split("\n");

                        for (String keyValue : keyValuePairs) {
                            String[] keyValueSplit = keyValue.split("=");
                            if (keyValueSplit.length == 2) {
                                String k = keyValueSplit[0].trim().toLowerCase();
                                String v = keyValueSplit[1].trim().toLowerCase();
                                if (k.equals("stockcode")) {
                                    out.collect(new Tuple2<>(v, 1));
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + value + ", cause: " + e.getMessage());
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStream
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
                        return Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1);
                    }
                });
                // .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                //     @Override
                //     public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) {
                //         // long windowStart = window.getStart();
                //         // long windowEnd = window.getEnd();
                //         // System.out.println(windowStart + "|" + windowEnd);
                //         try {
                //             int sum = 0;
                //             for (Tuple2<String, Integer> in : input) {
                //                 sum += in.f1;
                //             }
                //             out.collect(new Tuple2<>(key, sum));
                //         } catch (Exception e) {
                //             System.err.println("Error: " + input + ", cause: " + e.getMessage());
                //         }
                //     }
                // });

        dataStream.print();

        final class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, redisKey);
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> data) {
                return data.f1.toString();
            }
        }

        streamOperator.addSink(new RedisSink<Tuple2<String, Integer>>(
                new FlinkJedisPoolConfig.Builder()
                        .setHost(properties.getProperty("redis.host"))
                        .setPort(Integer.parseInt(properties.getProperty("redis.port")))
                        .setPassword(properties.getProperty("redis.password"))
                        .setDatabase(Integer.parseInt(properties.getProperty("redis.db")))
                        .build(),
                new RedisWordCountMapper()));

        env.execute("flink.datastream.GetKafkaToMysql");
    }
}