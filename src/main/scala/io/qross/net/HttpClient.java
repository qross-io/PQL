package io.qross.net;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {

    public static String request(String url) {
        return request(url, "");
    }

    public static String get(String url, String userName, String password) throws IOException {

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(60000)
                .setConnectTimeout(60000)
                .setConnectionRequestTimeout(60000)
                .build();

        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("X-AUTH-USERNAME", userName);
        httpGet.setHeader("X-AUTH-PASSWORD", password);

        CloseableHttpClient httpClient = HttpClients.createDefault();
        httpGet.setConfig(requestConfig);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity, "UTF-8");
    }

    public static String request(String url, String post) {

        StringBuilder buffer = new StringBuilder();
        try {
            if (post == null){ post = "";}
            String method = post.isEmpty() ? "GET" : "POST";

            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            //conn.addRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.setRequestMethod(method);
            conn.connect();

            // write content to server, parameters for http request
            if(!post.isEmpty()){
                OutputStream os = conn.getOutputStream();
                os.write(post.getBytes("utf-8"));
                os.close();
            }

            // read response content
            InputStream is = conn.getInputStream();
            InputStreamReader isr = new InputStreamReader(is,"utf-8");
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                buffer.append(line);
            }
            br.close();
            isr.close();
            is.close();
         }
         catch(Exception e) {
            e.printStackTrace();
         }

         return buffer.toString();
    }

    public static String postFile(String url, String filePath, String data) throws IOException {

        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            System.err.println("File not found!");
        }

         HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setUseCaches(false); // cant't cache when use post method

        conn.setRequestProperty("Connection", "Keep-Alive");
        conn.setRequestProperty("Charset", "UTF-8");
        // border needed
        String BOUNDARY = "----------" + System.currentTimeMillis();
        conn.setRequestProperty("Content-Type", "multipart/form-data; boundary="+ BOUNDARY);
        // main body
        // first part
        StringBuilder sb = new StringBuilder();
        sb.append("--"); // 必须多两道线
        sb.append(BOUNDARY);
        sb.append("\r\n");
        sb.append("Content-Disposition: form-data;name=\"file\";filename=\""+ file.getName() + "\"\r\n");
        sb.append("Content-Type:application/octet-stream\r\n\r\n");
        byte[] head = sb.toString().getBytes("utf-8");

        OutputStream out = new DataOutputStream(conn.getOutputStream());
        out.write(head);
        out.write(data.getBytes("utf-8"));
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        int bytes = 0;
        byte[] bufferOut = new byte[1024];
        while ((bytes = in.read(bufferOut)) != -1) {
            out.write(bufferOut, 0, bytes);
        }
        in.close();
        byte[] foot = ("\r\n--" + BOUNDARY + "--\r\n").getBytes("utf-8");
        out.write(foot);
        out.flush();
        out.close();

        StringBuffer buffer = new StringBuffer();
        String result = null;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
            }

            result = buffer.toString();
        } catch (IOException e) {
            System.out.println("Post exception! " + e);
            e.printStackTrace();
            throw new IOException("Response exception!");
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return result;
    }
}
