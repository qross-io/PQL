package io.qross.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.qross.core.DataHub;
import io.qross.ext.TypeExt;
import io.qross.jdbc.DataAccess;
import io.qross.jdbc.JDBC;
import io.qross.net.HttpRequest;
import io.qross.net.Json;
import io.qross.pql.PQL;
import io.qross.time.DateTime;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OneApiRequester {

    private int userId = 0;
    private String username = "anonymous";
    private String role = "";
    private final Map<String, Object> info = new HashMap<>();

    public OneApiRequester() {

    }

    public OneApiRequester signIn(Map<String, Object> info) {
        this.info.putAll(info);

        if (!this.info.containsKey("userid") || this.info.containsKey("username") || !this.info.containsKey("role")) {
            for (String key : this.info.keySet()) {
                switch(key.toLowerCase()) {
                    case "userid":
                    case "uid":
                    case "id":
                        if (!this.info.containsKey("userid")) {
                            this.info.put("userid", this.info.get(key));
                        }
                        break;
                    case "username":
                    case "name":
                        if (!this.info.containsKey("username")) {
                            this.info.put("username", this.info.get(key));
                        }
                        break;
                    case "role":
                        if (!this.info.containsKey("role")) {
                            this.info.put("role", this.info.get(key));
                        }
                        break;
                }
            }
        }

        if (this.info.containsKey("userid")) {
            this.userId = Integer.parseInt(this.info.get("userid").toString());
        }
        if (this.info.containsKey("username")) {
            this.username = this.info.get("username").toString();
        }
        if (this.info.containsKey("role")) {
            this.role = this.info.get("role").toString();
        }

        return this;
    }

    public OneApiRequester signIn(int userId, String userName, String role) {
        this.userId = userId;
        this.username = userName;
        this.role = role;
        return this;
    }

    public OneApiRequester signIn(int userId, String userName, String role, Map<String, Object> info) {
        this.userId = userId;
        this.username = userName;
        this.role = role;
        this.info.putAll(info);
        return this;
    }

    public Object request(String path, String connectionName) {
        return this.request(path, new DataHub(connectionName));
    }

    public Object request(String path){
        return this.request(path, DataHub.DEFAULT());
    }

    public Map<String, Object> request(String path, DataHub dh) {

        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", DateTime.now().getString("yyyy-MM-dd HH:mm:ss"));
        result.put("code", 200);
        result.put("data", null);
        result.put("elapsed", -1L);

        long mark = System.currentTimeMillis();

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();

            String method = request.getMethod();
            if (!OneApi.contains(path, method)) {
                OneApi.readAll();
            }

            if (OneApi.contains(path, method)) {
                OneApiPlain api = OneApi.pick(path, method);

                boolean allowed = false;
                if (Setting.OneApiSecurityMode.equals("none")) {
                    allowed = true;
                }
                else if (Setting.OneApiSecurityMode.equals("token")) {
                    String token = request.getParameter("token");
                    //token
                    if (token != null && !token.isEmpty()) {
                        if (OneApi.authenticateToken(method, path, token)) {
                            allowed = true;
                        }
                    }
                    //anonymous
                    else if (OneApi.authenticateAnonymous(method, path)) {
                        allowed = true;
                    }
                }
                else if (Setting.OneApiSecurityMode.equals("secret")) {
                    String key = request.getParameter("secret");
                    //secret key
                    if (key != null && !key.isEmpty()) {
                        if (OneApi.authenticateSecretKey(method, path, key)) {
                            allowed = true;
                        }
                    }
                    //anonymous
                    else if (OneApi.authenticateAnonymous(method, path)) {
                        allowed = true;
                    }
                }
                else if (Setting.OneApiSecurityMode.equals("user")) {
                    //user
                    if (OneApi.authenticateUser(method, path, username, role)) {
                        allowed = true;
                    }
                    //anonymous
                    else if (OneApi.authenticateAnonymous(method, path)) {
                        allowed = true;
                    }
                }

                if (allowed) {
                    HttpRequest http = new HttpRequest(request);
                    Map<String, Object> parameters = http.getParameters();
                    try {
                        result.put("data", new PQL(api.statement, dh)
                                .signIn(userId, username, role, info)
                                .place(parameters)
                                .placeDefault(api.defaultValues)
                                .set("request", http.getRequestInfo())
                                .run());

                        result.put("message", "");
                        result.put("elapsed", System.currentTimeMillis() - mark);
                        result.put("status", "success");

                        if (api.exampled) {
                            OneApi.saveExample(api.id, new ObjectMapper().writeValueAsString(result.get("data")));
                            api.exampled = false;
                        }
                    }
                    catch(Exception e) {
                        result.put("message", TypeExt.ExceptionExt(e).getReferMessage());
                        result.put("status", "exception");
                        result.put("code", 500);
                    }

                    if (OneApi.TRAFFIC_GRADE > 0) {
                        count(userId, api.id, parameters, (long) result.get("elapsed"));
                    }
                }
                else {
                    result.put("message", "Access denied.");
                    result.put("status", "denied");
                    result.put("code", 403);
                }
            }
            else {
                result.put("message", "Incorrect or miss '" + method + ":" + path + "'");
                result.put("status", "incorrect-method-or-path");
                result.put("code", 404);
            }
        }
        else {
            result.put("message", "Need Spring Boot environment.");
            result.put("status", "spring-boot-required");
            result.put("code", 503);
        }

        return result;
    }

    public static ConcurrentLinkedQueue<OneApiPageView> TRAFFIC = new ConcurrentLinkedQueue<>();
    private static long TRAFFIC_TIMESTAMP = System.currentTimeMillis();

    private static void count(int userId, int apiId, Map<String, Object> parameters, long elapsed) {
        //cache
        TRAFFIC.add(new OneApiPageView(userId, apiId, parameters, elapsed));
        //record
        if (TRAFFIC.size() >= 1000 || System.currentTimeMillis() - TRAFFIC_TIMESTAMP > 60000) {

            List<OneApiPageView> pageViews = new ArrayList<>(TRAFFIC);
            TRAFFIC.clear();

            DataAccess ds = new DataAccess(JDBC.QROSS());

            //summary
            ds.setBatchCommand("INSERT INTO qross_api_traffic_summary (userid, service_id, api_id, stat_date, stat_hour, times) VALUES (?, ?, ?, ?, ?, 1) ON DUPLICATE KEY UPDATE times=times+1");
            for (OneApiPageView pv : pageViews) {
                ds.addBatch(userId, OneApi.SERVICE_ID, pv.apiId, pv.requestTime.substring(0, 10), pv.requestTime.substring(11, 13));
            }
            ds.executeBatchUpdate();

            //details
            if (OneApi.TRAFFIC_GRADE > 1) {
                ds.setBatchCommand("INSERT INTO qross_api_traffic_details (userid, service_id, api_id, elapsed, request_time) VALUES (?, ?, ?, ?, ?);");
                for (OneApiPageView pv : pageViews) {
                    ds.addBatch(userId, OneApi.SERVICE_ID, pv.apiId, pv.elapsed, pv.requestTime);
                }
                ds.executeBatchUpdate();
            }

            //queries summary
            if (OneApi.TRAFFIC_GRADE > 2) {
                ds.setBatchCommand("INSERT INTO qross_api_traffic_queries_summary (userid, service_id, api_id, query_name, query_value, stat_date, times) VALUES (?, ?, ?, ?, ?, ?, 1) ON DUPLICATE KEY UPDATE times=times+1");
                for (OneApiPageView pv : pageViews) {
                    for (Map.Entry<String, Object> entry : pv.parameters.entrySet()) {
                        ds.addBatch(userId, OneApi.SERVICE_ID, pv.apiId, entry.getKey(), entry.getValue(), pv.requestTime.substring(0, 10));
                    }
                }
                ds.executeBatchUpdate();
            }

            //queries details
            if (OneApi.TRAFFIC_GRADE > 3) {
                ds.setBatchCommand("INSERT INTO qross_api_traffic_queries_details (userid, service_id, api_id, query_name, query_value, request_time) VALUES (?, ?, ?, ?, ?, ?)");
                for (OneApiPageView pv : pageViews) {
                    for (Map.Entry<String, Object> entry : pv.parameters.entrySet()) {
                        ds.addBatch(userId, OneApi.SERVICE_ID, pv.apiId, entry.getKey(), entry.getValue(), pv.requestTime);
                    }
                }
                ds.executeBatchUpdate();
            }

            ds.close();

            TRAFFIC_TIMESTAMP = System.currentTimeMillis();
        }
    }
}