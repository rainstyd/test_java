package flink.table;

import flink.utils.GetConfigFromFile;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;


public class GetHdfsToHdfs {

    public static void main(String[] args) throws Exception {

        // bin/flink run -c flink.table.GetHdfsToHdfs rainsty/test_flink-1.0-SNAPSHOT.jar --config=rainsty/test_flink.properties

        GetConfigFromFile properties = new GetConfigFromFile(args);
        System.out.println(properties.getProperty("hdfs.host", "ERROR: 读取配置文件异常！"));

        String address = properties.getProperty("hdfs.host") + ":" + properties.getProperty("hdfs.port");

        String selectFilePath = "hdfs://" + address + "/user/rainsty/test_table.csv";
        // String selectFilePath = "D:\\Files\\test_table.csv";
        String insertFilePath = "hdfs://" + address + "/user/rainsty/test_table";
        // String insertFilePath = "D:\\Files\\test_table";

        System.out.println(selectFilePath);
        System.out.println(insertFilePath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        final Schema selectSchema = Schema.newBuilder()
                .column("a", DataTypes.STRING())
                .column("b", DataTypes.INT())
                .column("c", DataTypes.INT())
                .column("d", DataTypes.INT())
                .column("e", DataTypes.DECIMAL(20, 6))
                .build();

        tableEnv.createTemporaryTable("test_table", TableDescriptor.forConnector("filesystem")
                .schema(selectSchema)
                .option("path", selectFilePath)
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        final Schema insertSchema = Schema.newBuilder()
                .column("sum_e", DataTypes.DECIMAL(20, 6))
                .build();

        tableEnv.createTemporaryTable("test_table_result", TableDescriptor.forConnector("filesystem")
                .schema(insertSchema)
                .option("path", insertFilePath)
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        Table table = tableEnv.sqlQuery("SELECT e sum_e FROM test_table");

        table.insertInto("test_table_result").execute();
    }
}
