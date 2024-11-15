// package test;
//
// import org.apache.commons.codec.binary.Base64;
// import org.apache.http.HttpHeaders;
// import org.apache.http.client.methods.CloseableHttpResponse;
// import org.apache.http.client.methods.HttpPut;
// import org.apache.http.entity.FileEntity;
// import org.apache.http.entity.StringEntity;
// import org.apache.http.impl.client.CloseableHttpClient;
// import org.apache.http.impl.client.DefaultRedirectStrategy;
// import org.apache.http.impl.client.HttpClientBuilder;
// import org.apache.http.impl.client.HttpClients;
// import org.apache.http.util.EntityUtils;
//
// import java.io.File;
// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
//
//
// public class Test {
//     private final static String HOST = "";
//     private final static int PORT = 18030;
//     private final static String DATABASE = "a"; // 数据库名
//     private final static String TABLE = "a"; // 数据表名
//     private final static String USER = "a"; // Doris 用户名
//     private final static String PASSWD = "a"; // Doris 密码
//     private final static String LOAD_FILE_NAME = "src/main/resources/data.txt"; // 本地文件路径
//
//     private final static String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
//             HOST, PORT, DATABASE, TABLE);
//
//     private final static HttpClientBuilder httpClientBuilder = HttpClients
//             .custom()
//             .setRedirectStrategy(new DefaultRedirectStrategy() {
//                 @Override
//                 protected boolean isRedirectable(String method) {
//                     // If the connection target is FE, you need to handle 307 redirect.
//                     return true;
//                 }
//             });
//
//     public void load(File file) throws Exception {
//         try (CloseableHttpClient client = httpClientBuilder.build()) {
//             HttpPut put = new HttpPut(loadUrl);
//             put.setHeader(HttpHeaders.EXPECT, "100-continue");
//             put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));
//
//             // 设置stream load 的label, 用于避免重复导入数据
//             put.setHeader("label","label-example");
//             //设置字段分隔符
//             put.setHeader("column_separator",",");
//
//             // Set the import file.
//             // StringEntity can also be used here to transfer arbitrary data.
//             // FileEntity entity = new FileEntity(file);
//             // put.setEntity(entity);
//
//             String jsonData = "[{ \"uniqueid\": \"test_device_6213\", \"provider\": \"a\" }]";
//             StringEntity entity = new StringEntity(jsonData, "UTF-8");
//             put.setEntity(entity);
//
//             try (CloseableHttpResponse response = client.execute(put)) {
//                 String loadResult = "";
//                 if (response.getEntity() != null) {
//                     loadResult = EntityUtils.toString(response.getEntity());
//                 }
//
//                 final int statusCode = response.getStatusLine().getStatusCode();
//                 if (statusCode != 200) {
//                     throw new IOException(
//                             String.format("Stream load failed. status: %s load result: %s", statusCode, loadResult));
//                 }
//
//                 System.out.println("Get load result: " + loadResult);
//             }
//         }
//     }
//
//     private String basicAuthHeader(String username, String password) {
//         final String tobeEncode = username + ":" + password;
//         byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
//         return "Basic " + new String(encoded);
//     }
//
//     public static void main(String[] args) throws Exception{
//         Test loader = new Test();
//         File file = new File(LOAD_FILE_NAME);
//         loader.load(file);
//     }
// }
//
// //
// // package test;
// // /**
// //  * 从指定的socket读取数据，对单词进行计算，将结果写入到Redis中
// //  */
// // public class Test {
// //     public static void main(String[] args) throws Exception {
// //         //创建Flink流计算执行环境
// //         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// //         //创建DataStream
// //         //Source
// //         DataStreamSource<String> lines = env.socketTextStream("node01", 9999);
// //         //调用Transformation开始
// //         SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
// //             @Override
// //             public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
// //                 String[] words = line.split(" ");
// //                 for (String word : words) {
// //                     //new Tuple2<String, Integer>(word, 1)
// //                     collector.collect(Tuple2.of(word, 1));
// //                 }
// //             }
// //         });
// //
// //         //分组
// //         KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
// //             @Override
// //             public String getKey(Tuple2<String, Integer> tp) throws Exception {
// //                 return tp.f0;
// //             }
// //         });
// //
// //         //聚合
// //         SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
// //
// //         //Transformation结束
// //
// //         //调用Sink
// //         //summed.addSink()
// //         FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("node03").setPassword("123456").setDatabase(8).build();
// //
// //         summed.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisWordCountMapper()));
// //         //启动执行
// //         env.execute("StreamingWordCount");
// //
// //     }
// //
// //     public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {
// //
// //         @Override
// //         public RedisCommandDescription getCommandDescription() {
// //             return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
// //         }
// //
// //         @Override
// //         public String getKeyFromData(Tuple2<String, Integer> data) {
// //             return data.f0;
// //         }
// //
// //         @Override
// //         public String getValueFromData(Tuple2<String, Integer> data) {
// //             return data.f1.toString();
// //         }
// //     }
// // }
// // //
// // // public class Test {
// // //     public static void main(String[] args) {
// // //         System.out.println("Hello World!");
// // //     }
// // // }
// // // import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// // // import org.apache.flink.api.common.functions.MapFunction;
// // // import org.apache.flink.api.common.restartstrategy.RestartStrategies;
// // // import org.apache.flink.api.common.time.Time;
// // // import org.apache.flink.api.java.tuple.Tuple2;
// // // import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
// // // import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
// // // import org.apache.flink.connector.jdbc.JdbcSink;
// // // import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
// // // import org.apache.flink.connector.kafka.source.KafkaSource;
// // // import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// // // import org.apache.flink.connector.kafka.source.enumerator.startup.StartupOptions;
// // // import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
// // // import org.apache.flink.streaming.api.CheckpointingMode;
// // // import org.apache.flink.streaming.api.datastream.DataStream;
// // // import org.apache.flink.streaming.api.environment.CheckpointConfig;
// // // import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// // // import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
// // // import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
// // // import org.apache.flink.streaming.api.windowing.time.Time;
// // // import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
// // // import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
// // // import org.apache.flink.util.Collector;
// // //
// // // import java.sql.PreparedStatement;
// // // import java.time.Duration;
// // //
// // // public class FlinkKafkaToMySQLExample {
// // //
// // //     public static void main(String[] args) throws Exception {
// // //
// // //         // 设置执行环境
// // //         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// // //         env.setParallelism(1);
// // //
// // //         // 配置 Kafka Source
// // //         KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
// // //                 .setBootstrapServers("localhost:9092")
// // //                 .setTopics("your_topic")
// // //                 .setGroupId("flink_consumer_group")
// // //                 .setStartingOffsets(OffsetsInitializer.latest())
// // //                 .setWatermarkStrategy(WatermarkStrategy.noWatermarks())
// // //                 .setDeserializer(new SimpleStringSchema())
// // //                 .build();
// // //
// // //         // 从 Kafka 读取数据
// // //         DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
// // //
// // //         // 将字符串映射为 (key, 1) 的形式
// // //         DataStream<Tuple2<String, Integer>> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
// // //             @Override
// // //             public Tuple2<String, Integer> map(String value) throws Exception {
// // //                 return new Tuple2<>(value, 1);
// // //             }
// // //         });
// // //
// // //         // 每五分钟进行一次窗口聚合
// // //         DataStream<Tuple2<String, Integer>> windowedStream = keyedStream
// // //                 .keyBy(value -> value.f0)
// // //                 .window(TumblingEventTimeWindows.of(Time.minutes(5)))
// // //                 .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
// // //                     @Override
// // //                     public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) {
// // //                         int sum = 0;
// // //                         for (Tuple2<String, Integer> in : input) {
// // //                             sum += in.f1;
// // //                         }
// // //                         out.collect(new Tuple2<>(key, sum));
// // //                     }
// // //                 });
// // //
// // //         // 配置 MySQL Sink
// // //         JdbcSink.SinkFunction<Tuple2<String, Integer>> jdbcSink = JdbcSink.sink(
// // //                 "INSERT INTO your_table (key, count) VALUES (?, ?)",
// // //                 (ps, t) -> {
// // //                     ps.setString(1, t.f0);
// // //                     ps.setInt(2, t.f1);
// // //                 },
// // //                 JdbcExecutionOptions.builder().withBatchSize(1000).build(),
// // //                 new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
// // //                         .withUrl("jdbc:mysql://localhost:3306/your_database")
// // //                         .withDriverName("com.mysql.cj.jdbc.Driver")
// // //                         .withUsername("your_username")
// // //                         .withPassword("your_password")
// // //                         .build()
// // //         );
// // //
// // //         // 将数据写入 MySQL
// // //         windowedStream.addSink(jdbcSink).name("MySQL Sink");
// // //
// // //         // 配置检查点
// // //         env.enableCheckpointing(30000); // 30秒
// // //         env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// // //         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// // //         env.getCheckpointConfig().setCheckpointTimeout(10000);
// // //         env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// // //         env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// // //
// // //         // 启动作业
// // //         env.execute("Flink Kafka to MySQL Example");
// // //     }
// // // }