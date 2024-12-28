package flink.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import flink.utils.GetConfigFromFile;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;


public class HostSearchJob {
    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.datastream.HostSearchJob rainsty/test_flink-1.0-SNAPSHOT.jar --config=rainsty/test_flink.properties
        /* 注释:
            CREATE TABLE IF NOT EXISTS hot_stock_code
            (
                `stock_code` VARCHAR(128) NOT NULL COMMENT "代码",
                `day` DATE NOT NULL COMMENT "日期",
                `total_number` BIGINT SUM DEFAULT "0" COMMENT "次数"
            )
            AGGREGATE KEY(`stock_code`, `day`)
            PARTITION BY RANGE(`day`) ()
            DISTRIBUTED BY HASH(`stock_code`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 3",
            "light_schema_change" = "true",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "7",
            "estimate_partition_size" = "2G"
            );


            CREATE ROUTINE LOAD rainsty.hot_stock_code_json ON hot_stock_code
            COLUMNS(stock_code,day,total_number)
            PROPERTIES(
                "format"="json",
                "jsonpaths"="[\"$.stockcode\",\"$.day\",\"$.total_number\"]"
            )
            FROM KAFKA(
                "kafka_broker_list" = "192.168.0.188:9094,192.168.0.188:9095,192.168.0.188:9096",
                "kafka_topic" = "HostSearchJobDes",
                "property.kafka_default_offsets" = "OFFSET_END"
            );


            CREATE TABLE IF NOT EXISTS active_user
            (
                `usersign` VARCHAR(128) NOT NULL COMMENT "用户",
                `day` DATE NOT NULL COMMENT "日期",
                `total_number` BIGINT SUM DEFAULT "0" COMMENT "次数"
            )
            AGGREGATE KEY(`usersign`, `day`)
            PARTITION BY RANGE(`day`) ()
            DISTRIBUTED BY HASH(`usersign`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 3",
            "light_schema_change" = "true",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "7",
            "estimate_partition_size" = "2G"
            );


            CREATE ROUTINE LOAD rainsty.active_user_json ON active_user
            COLUMNS(usersign,day,total_number)
            PROPERTIES(
                "format"="json",
                "jsonpaths"="[\"$.usersign\",\"$.day\",\"$.total_number\"]"
            )
            FROM KAFKA(
                "kafka_broker_list" = "192.168.0.188:9094,192.168.0.188:9095,192.168.0.188:9096",
                "kafka_topic" = "HostSearchJobDes",
                "property.kafka_default_offsets" = "OFFSET_END"
            );

            SHOW ROUTINE LOAD FOR rainsty.hot_stock_code_json;
            SHOW ROUTINE LOAD TASK WHERE jobname = 'hot_stock_code_json';

            SHOW ROUTINE LOAD FOR rainsty.active_user_json;
            SHOW ROUTINE LOAD TASK WHERE jobname = 'active_user_json';

        */

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("HostSearchJob.bootstrap.servers", "ERROR: 读取配置文件异常！"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(properties.getIntegerProperty("HostSearchJob.source_parallelism", "1"));
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("HostSearchJob.bootstrap.servers"))
                .setTopics(properties.getProperty("HostSearchJob.topic.name.src"))
                .setGroupId(properties.getProperty("HostSearchJob.group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<String> stringStream = sourceStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) { // 解析kafka日志内容
                try {
                    String[] parts = value.split("----------");
                    JSONObject jsonObjectFields = new JSONObject();
                    jsonObjectFields.put("action", 1);
                    jsonObjectFields.put("stockcode", 1);
                    jsonObjectFields.put("mobilecode", 1);
                    jsonObjectFields.put("uniqueid", 1);
                    jsonObjectFields.put("code", 1);

                    for (String part : parts) {
                        String[] keyValuePairs = part.split("\n");
                        JSONObject jsonObject = new JSONObject();

                        for (String keyValue : keyValuePairs) {
                            String[] keyValueSplit = keyValue.split("=");
                            if (keyValueSplit.length == 2) {
                                String k = keyValueSplit[0].trim().toLowerCase();
                                if (jsonObjectFields.has(k)) {
                                    String v = keyValueSplit[1].trim().toLowerCase();
                                    jsonObject.put(k.toLowerCase(), v);
                                }
                            }
                        }

                        if (jsonObject.optString("action", "").equals("20109") || jsonObject.optString("action", "").equals("10001")){
                            if (jsonObject.optString("action", "").equals("10001") && !jsonObject.optString("code", "").isEmpty()) {
                                jsonObject.put("stockcode", jsonObject.optString("code", ""));
                                jsonObject.remove("code");
                            }
                            if (jsonObject.optString("mobilecode", "").isEmpty() && !jsonObject.optString("uniqueid", "").isEmpty()) {
                                jsonObject.put("usersign", jsonObject.get("uniqueid"));
                            } else {
                                jsonObject.put("usersign", jsonObject.optString("mobilecode", ""));
                            }
                            jsonObject.put("total_number", 1);
                            jsonObject.put("day", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                            jsonObject.remove("action");
                            jsonObject.remove("mobilecode");
                            jsonObject.remove("uniqueid");
                            out.collect(jsonObject.toString());
                        }
                    }

                } catch (Exception e) {
                    System.err.println("Error parsing value: " + value + ", cause: " + e.getMessage());
                    out.collect(null);
                }
            }

        });

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("HostSearchJob.bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.getProperty("HostSearchJob.topic.name.des"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // stringStream.print();
        stringStream.sinkTo(kafkaSink).setParallelism(properties.getIntegerProperty("HostSearchJob.sink_parallelism", "1"));

        env.execute("flink.datastream.GetKafkaToKafka");
    }
}
