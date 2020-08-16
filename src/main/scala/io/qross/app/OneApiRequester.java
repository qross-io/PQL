package io.qross.app;

import io.qross.core.DataHub;
import io.qross.jdbc.DataAccess;
import io.qross.jdbc.JDBC;
import io.qross.net.HttpRequest;
import io.qross.pql.PQL;
import io.qross.time.DateTime;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public Object request(String path, DataHub dh) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();

            String method = request.getMethod();
            if (!OneApi.contains(path, method)) {
                OneApi.readAll();
            }

            if (OneApi.contains(path, method)) {
                OneApi api = OneApi.pick(path, method);

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
                    //count(api.path);
                    HttpRequest http = new HttpRequest(request);
                    return  new PQL(api.sentences, dh)
                                .signIn(userId, username, role, info)
                                .place(http.getParameters())
                                .place(api.defaultValue)
                                .set(" request", http.getRequestInfo())
                                .run();
                }
                else {
                    return "{\"error\": \"Access denied.\"}";
                }
            }
            else {
                return "{\"error\": \"WRONG or MISS path/Method '" + path + " " + method + "'\"}";
            }
        }
        else {
            return "{\"error\": \"Need spring boot environment.\"}";
        }
    }

    //to be review

    //service,path,date,hour -> pv
    public static List<String> TRAFFIC = new ArrayList<>();
    private static DateTime LastSaveTime = DateTime.now();

    //unsaved traffic data
    public static List<String> traffic() {
        return TRAFFIC;
    }

    //traffic count
    private static void count(String path) {
        if (!Setting.OneApiServiceName.isEmpty()) {
            TRAFFIC.add(path + DateTime.now().getString(",yyyy-MM-dd,HH"));
            if (TRAFFIC.size() >= 1000 || LastSaveTime.earlier(DateTime.now()) >= 60000) {
                DataAccess ds = new DataAccess(JDBC.QROSS());
                Map<String, Integer> traffic = new HashMap<>();
                for (String pv : TRAFFIC) {
                    if (traffic.containsKey(pv)) {
                        traffic.put(pv, traffic.get(pv) + 1);
                    }
                    else {
                        traffic.put(pv, 1);
                    }
                }
                ds.setBatchCommand("INSERT INTO qross_api_traffic (service_name, path, record_date, record_hour, pv) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE pv=pv+?");
                for (String key : traffic.keySet()) {
                    String[] values = key.split(",");
                    ds.addBatch(Setting.OneApiServiceName, values[0], values[1], values[2], traffic.get(key), traffic.get(key));
                }
                ds.executeBatchUpdate();
                ds.close();

                LastSaveTime = DateTime.now();
            }
        }
    }
}
