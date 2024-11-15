package flink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;


public class DorisHttpSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
    private GetConfigFromFile properties = null;
    private String tableName = "";
    private Object dorisHttpConnect = null;
    private int batchSize = 10000; // 攒批大小
    private int flushIntervalMS = 10000; // 刷新间隔（毫秒）
    private transient List<String> buffer;
    private transient long lastFlushTimestamp;

    public DorisHttpSink(GetConfigFromFile properties, String tableName, Integer batchSize, Integer flushIntervalMS) {
        this.properties = properties;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.flushIntervalMS = flushIntervalMS;
        this.dorisHttpConnect = null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.buffer = new ArrayList<>();
        this.lastFlushTimestamp = System.currentTimeMillis();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        synchronized (buffer) {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush();
            } else {
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastFlushTimestamp >= flushIntervalMS) {
                    flush();
                }
            }
        }
    }

    private void flush() {
        List<String> localBuffer;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            localBuffer = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTimestamp = System.currentTimeMillis();
        }

        new Thread(() -> { // 在另一个线程中执行写入操作，避免阻塞Flink的main线程
            try {
                System.out.println(localBuffer);
                writeToDoris(localBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private Object getDorisHttpConnect() {

        if (dorisHttpConnect == null) {
            this.dorisHttpConnect = new DorisHttpConnect(properties, tableName);
        }
        return dorisHttpConnect;
    }

    private void writeToDoris(List<String> data) throws Exception {
        DorisHttpConnect dc = (DorisHttpConnect) getDorisHttpConnect();

        try {
            JSONArray jsonArray = new JSONArray();

            for (String d: data) {
                JSONObject j = new JSONObject(d);
                jsonArray.put(j);
            }

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("data", jsonArray);
            JSONObject response = dc.sendHttpPut(jsonObject.toString());

            if (response.get("Status").equals("Success")) {
                System.out.println("Insert doris success!");
            } else {
                System.out.println("ERROR: Insert doris failed! " + response);
            }

        } catch (Exception e) {
            System.err.println("Error" + "cause: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        flush(); // 确保在关闭时刷新剩余数据
    }

}
