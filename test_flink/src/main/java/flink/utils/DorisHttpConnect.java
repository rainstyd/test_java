package flink.utils;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class DorisHttpConnect {
    private final List<String> dorisNodes;
    private final GetConfigFromFile properties;
    private final Properties newProperties;
    private final String urlPath;

    public DorisHttpConnect(GetConfigFromFile properties, String tableName) {
        /* Doris http 请求相关配置内容
            doris.http.urls=http://0.0.0.0:18030,http://0.0.0.0:18030
            doris.user=user
            doris.password=password
            doris.database=database
            doris.redirect=false
            doris.redirect.address=192.168.0.45:18040->0.0.0.0:18040,192.168.0.46:18040->0.0.0.0:18040
         */
        this.dorisNodes = new ArrayList<>(Arrays.asList(properties.getProperty("doris.http.urls").split(",")));
        this.properties = properties;
        this.newProperties = new Properties();
        this.urlPath = "/api/" + properties.getProperty("doris.database") + "/" + tableName + "/_stream_load";

        String[] redirectAddressList = properties.getProperty("doris.redirect.address").split(",");
        for (String address: redirectAddressList) { // 解析重定向配置
            String[] addressList = address.split("->");
            this.newProperties.setProperty(addressList[0], addressList[1]);
        }
    }

    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // 开启自动重定向，为内网地址，外网访问不可行
                    // 关闭自动重定向，手动切换为外网地址访问可行
                    return Boolean.parseBoolean(properties.getProperty("doris.redirect"));
                }
            });

    private HttpPut SetHttpRequestHeaders(HttpPut httpPut, String jsonData) {
        String auth = properties.getProperty("doris.user") + ":" + properties.getProperty("doris.password");
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        httpPut.setHeader("Authorization", "Basic " + encodedAuth);
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPut.setHeader("'label'", UUID.randomUUID().toString());
        httpPut.setHeader("Content-Type", "application/json");
        httpPut.setHeader("format", "json");
        httpPut.setHeader("strip_outer_array", "true");
        httpPut.setHeader("json_root", "$.data");
        httpPut.setHeader("allow_redirects", "false");
        StringEntity entity = new StringEntity(jsonData, "UTF-8");
        httpPut.setEntity(entity);
        return httpPut;
    }

    public JSONObject sendHttpPut(String jsonData) throws IOException {
        String responseBody = "";

        for (String url : dorisNodes) {
            url = url + urlPath;

            try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
                HttpPut httpPutObj = new HttpPut(url);
                HttpPut httpPut = SetHttpRequestHeaders(httpPutObj, jsonData);

                try (CloseableHttpResponse response = httpClient.execute(httpPut)) {

                    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_TEMPORARY_REDIRECT) {
                        Header locationHeader = response.getFirstHeader("Location");

                        if (locationHeader != null) {
                            HttpPut newHttPutObj = new HttpPut(locationHeader.getValue());
                            HttpPut newHttPut = SetHttpRequestHeaders(newHttPutObj, jsonData);
                            String redirectUrl = String.valueOf(newHttPut.getURI());
                            String address = redirectUrl.split("@")[1].split("/")[0];
                            String newUrl = "http://" + newProperties.getProperty(address) + urlPath;
                            newHttPut.setURI(URI.create(newUrl));
                            CloseableHttpResponse newResponse = httpClient.execute(newHttPut);
                            responseBody = EntityUtils.toString(newResponse.getEntity(), StandardCharsets.UTF_8);
                        }
                    } else {
                        responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    }
                }
                return new JSONObject(responseBody);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Failed to send http put: " + e.getMessage());
            }
        }
        return null;
    }
}
