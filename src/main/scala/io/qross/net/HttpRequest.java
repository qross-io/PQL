package io.qross.net;

import io.qross.core.DataRow;
import io.qross.core.DataType;
import io.qross.net.Json;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class HttpRequest {

    private HttpServletRequest request = null;

    public HttpRequest() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            this.request = attributes.getRequest();
        }
    }

    public HttpRequest(HttpServletRequest request) {
        this.request = request;
    }

    public Map<String, Object> getParameters() {

        Map<String, Object> params = new HashMap<>();

        Map<String, String[]> map = request.getParameterMap();
        for (String key : map.keySet()) {
            String[] values = map.get(key);
            if (values.length > 0 && !key.startsWith("r0x")) {
                params.put(key, values[0]);
            }
        }

        //json params
        StringBuilder sb = new StringBuilder();
        try {
            // body stream
            BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (sb.length() > 0) {
            params.putAll(new Json(sb.toString()).parseJavaMap("/"));
        }

        return params;
    }

    public DataRow getRequestInfo() {
        DataRow info = new DataRow();
        info.set("method", request.getMethod(), DataType.TEXT());
        info.set("url", request.getRequestURL().toString(), DataType.TEXT());
        info.set("protocol", request.getScheme().toUpperCase(), DataType.TEXT());
        info.set("domain", request.getServerName(), DataType.TEXT());
        info.set("host", request.getServerName(), DataType.TEXT());
        info.set("port", request.getServerPort(), DataType.INTEGER());
        info.set("path", request.getRequestURI(), DataType.TEXT());
        info.set("queryString", request.getQueryString(), DataType.TEXT());

        return info;
    }
}
