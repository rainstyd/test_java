package flink.datastream;

import flink.utils.DorisHttpConnect;
import flink.utils.GetConfigFromFile;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import flink.utils.DorisHttpSink;
import org.json.JSONObject;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class GetKafkaToDoris {
    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.datastream.GetKafkaToDoris rainsty/test_flink-1.0-SNAPSHOT.jar --config=rainsty/test_flink.properties
        /* kafka source data (start line do not have Space)
            正常:<13:12:32.303>45673BD6BC4[1]
            stockcode=1
            b=2
            ----------异常:<13:12:32.303>45673BD6BA3[0]
            stockcode=3
            d=4

            CREATE TABLE `rainsty`.`test` (
                `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT 'ID',
                `name` varchar(32) NOT NULL COMMENT '姓名',
                `age` int(11) NOT NULL COMMENT '年龄'
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 6;
        */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("doris.http.urls", "ERROR: 读取配置文件异常！"));

        // String jsonData = "{\"data\": [{ \"uniqueid\": \"test_device_62133_5\", \"provider\": \"aaaaa_5\" }]}";
        // DorisHttpConnect client = new DorisHttpConnect(properties, "app");
        //
        // try {
        //     JSONObject response = client.sendHttpPut(jsonData);
        //     System.out.println(response);
        // } catch (IOException e) {
        //     System.err.println("Error" + "cause: " + e.getMessage());
        // }

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics(properties.getProperty("topic.name.src"))
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<String> dataStream = sourceStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) { // 解析kafka日志内容
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
                                    JSONObject jsonObject = new JSONObject(); // 模拟测试数据
                                    jsonObject.put("uniqueid", UUID.randomUUID().toString());
                                    jsonObject.put("provider", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                                    out.collect(jsonObject.toString());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + value + ", cause: " + e.getMessage());
                }
            }
        });

        dataStream.addSink(new DorisHttpSink(properties, "app", 10000, 10000));

        env.execute("flink.datastream.GetKafkaToDoris");
    }
}
