package flink.datastream;

import flink.utils.GetConfigFromFile;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class GetMysqlToMysql {
    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.datastream.GetMysqlToMysql rainsty/test_java-1.0-SNAPSHOT.jar --config=rainsty/test_java.properties
        /*
        CREATE TABLE `test` (
          `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ID',
          `name` varchar(32) COLLATE utf8mb4_general_ci NOT NULL COMMENT '姓名',
          `age` int NOT NULL COMMENT '年龄',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=193 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("mysql.host", "ERROR: 读取配置文件异常！"));

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://"
                        + properties.getProperty("mysql.host")
                        + ":"
                        + properties.getProperty("mysql.port")
                        + "/"
                        + properties.getProperty("mysql.database"))
                .setUsername(properties.getProperty("mysql.user"))
                .setPassword(properties.getProperty("mysql.password"))
                .setQuery("SELECT id, name, age FROM test")
                .setRowTypeInfo((RowTypeInfo) Types.ROW_NAMED(new String[]{"id", "name", "age"}, Types.LONG, Types.STRING, Types.INT))
                .finish();

        DataStream<Row> sourceStream = env.createInput(jdbcInputFormat);

        DataStream<Row> dataStream = sourceStream.map(row -> {
            try {
                Integer newAge = (Integer) row.getField(2);
                newAge += 1;
                Row newRow = new Row(3);
                newRow.setField(0, row.getField(0));
                newRow.setField(1, row.getField(1));
                newRow.setField(2, newAge);

                System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                System.out.println(newRow); // 测试打印

                return newRow;
            } catch (Exception e) {
                System.err.println("Error parsing value: " + row + ", cause: " + e.getMessage());
                return null;
            }
        });

        // dataStream.print();

        String insertSql = "INSERT INTO test (name, age) VALUES (?, ?)";

        JdbcStatementBuilder<Row> statementBuilder = (ps, t) -> {
            try {
                String name = (String) t.getField(1);
                Integer age = (Integer) t.getField(2);
                ps.setString(1, name);
                ps.setInt(2, age);
            } catch (Exception e) {
                System.err.println("Error parsing value: " + t + ", cause: " + e.getMessage());
            }
        };

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withMaxRetries(10)
                .withBatchIntervalMs(1000)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://"
                        + properties.getProperty("mysql.host")
                        + ":"
                        + properties.getProperty("mysql.port")
                        + "/"
                        + properties.getProperty("mysql.database"))
                .withUsername(properties.getProperty("mysql.user"))
                .withPassword(properties.getProperty("mysql.password"))
                .build();

        dataStream.addSink(JdbcSink.sink(insertSql, statementBuilder, executionOptions, connectionOptions));

        env.execute("flink.datastream.GetMysqlToMysql");
    }
}
