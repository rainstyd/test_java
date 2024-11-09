package flink.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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


public class GetKafkaToKafka {
    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.datastream.GetKafkaToKafka rainsty/test_java-1.0-SNAPSHOT.jar --config=rainsty/test_java.properties
        /* kafka source data (start line do not have Space)
            正常:<13:12:32.303>45673BD6BC4[1]
            a=1
            b=2
            ----------异常:<13:12:32.303>45673BD6BA3[0]
            c=3
            d=4
        */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("bootstrap.servers", "ERROR: 读取配置文件异常！"));

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics(properties.getProperty("topic.name.src"))
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<String> stringStream = sourceStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) { // 解析kafka日志内容
                try {
                    JSONArray jsonArray = new JSONArray();
                    String[] parts = value.split("----------");

                    for (String part : parts) {
                        JSONObject jsonObject = getJsonObject(part);
                        jsonArray.put(jsonObject);
                    }

                    System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    // System.out.println(jsonArray.toString()); // 测试打印

                    return jsonArray.toString();
                } catch (Exception e) {
                    System.err.println("Error parsing value: " + value + ", cause: " + e.getMessage());
                    return null;
                }
            }

        // DataStream<String> stringStream = sourceStream.flatMap(new FlatMapFunction<String, String>() {
        //     @Override
        //     public void flatMap(String value, Collector<String> out) { // 解析kafka日志内容
        //         try {
        //             JSONArray jsonArray = new JSONArray();
        //             String[] parts = value.split("----------");
        //
        //             for (String part : parts) {
        //                 JSONObject jsonObject = getJsonObject(part);
        //                 jsonArray.put(jsonObject);
        //             }
        //
        //             System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        //             // System.out.println(jsonArray.toString()); // 测试打印
        //
        //             out.collect(jsonArray.toString());
        //         } catch (Exception e) {
        //             System.err.println("Error parsing value: " + value + ", cause: " + e.getMessage());
        //             out.collect(null);
        //         }
        //     }

            private JSONObject getJsonObject(String part) {
                String[] keyValuePairs = part.split("\n");
                JSONObject jsonObject = new JSONObject();

                String firstElement = keyValuePairs[0];
                String logType = firstElement.substring(0, 2);
                String uniqueStr = firstElement.split(">")[1].split("\\[")[0];
                jsonObject.put("logType".toLowerCase(), logType);
                jsonObject.put("uniqueStr".toLowerCase(), uniqueStr);

                for (String keyValue : keyValuePairs) {
                    String[] keyValueSplit = keyValue.split("=");
                    if (keyValueSplit.length == 2) {
                        String k = keyValueSplit[0].trim();
                        String v = keyValueSplit[1].trim();
                        jsonObject.put(k.toLowerCase(), v);
                    }
                }
                return jsonObject;
            }

        });

        stringStream = stringStream.filter(Objects::nonNull);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.getProperty("topic.name.des"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        stringStream.sinkTo(kafkaSink);

        env.execute("flink.datastream.GetKafkaToKafka");
    }
}
