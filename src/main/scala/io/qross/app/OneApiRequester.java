package io.qross.app;

import io.qross.core.DataHub;
import io.qross.jdbc.DataAccess;
import io.qross.net.Json;
import io.qross.pql.PQL;
import io.qross.time.DateTime;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneApiRequester {

    private String path = "";
    private DataHub dh = DataHub.DEFAULT();
    private final Map<String, String> params = new HashMap<>();
    private int userId = 0;
    private String userName = "anonymous";
    private String role = "";
    private final Map<String, Object> info = new HashMap<>();

    public OneApiRequester() {

    }

    public OneApiRequester(String path) {
        this.path = path;
    }

    public OneApiRequester(String path, Map<String, String> params) {
        this.path = path;
        this.params.putAll(params);
    }

    public OneApiRequester(String path, String connectionName) {
        this.path = path;
        this.dh = new DataHub(connectionName);
    }

    public OneApiRequester(String path, Map<String, String> params, String connectionName) {
        this.path = path;
        this.params.putAll(params);
        this.dh = new DataHub(connectionName);
    }

    public OneApiRequester(String path, DataHub dh) {
        this.path = path;
        this.dh = dh;
    }

    public OneApiRequester(String path, Map<String, String> params, DataHub dh) {
        this.path = path;
        this.params.putAll(params);
        this.dh = dh;
    }

    public OneApiRequester signIn(Map<String, Object> info) {
        this.info.putAll(info);
        return this;
    }

    public OneApiRequester signIn(int userId, String userName, String role) {
        this.userId = userId;
        this.userName = userName;
        this.role = role;
        return this;
    }

    public OneApiRequester signIn(int userId, String userName, String role, Map<String, Object> info) {
        this.userId = userId;
        this.userName = userName;
        this.role = role;
        this.info.putAll(info);
        return this;
    }

    public Object request(String path) {
        this.path = path;
        return this.request();
    }

    public Object request(String path, String connectionName) {
        this.path = path;
        this.dh = new DataHub(connectionName);

        return this.request();
    }

    public Object request(String path, DataHub dh){
        this.path = path;
        this.dh = dh;

        return this.request();
    }

    public Object request(String path, Map<String, String> params) {
        this.path = path;
        this.params.putAll(params);

        return this.request();
    }

    public Object request(String path, Map<String, String> params, String connectionName) {
        this.path = path;
        this.params.putAll(params);
        this.dh = new DataHub(connectionName);

        return this.request();
    }

    public Object request(String path, Map<String, String> params, DataHub dh) {
        this.path = path;
        this.params.putAll(params);

        return this.request();
    }

    public Object request() {
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
                    if (token == null || token.isEmpty()) {
                        // read token from cookie
                        for (Cookie cookie : request.getCookies()) {
                            if (cookie.getName().equalsIgnoreCase("token")) {
                                token = cookie.getValue();
                                break;
                            }
                        }
                    }

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
                else {
                    //user
                    if (OneApi.authenticateRole(method, path, role)) {
                        allowed = true;
                    }
                    //anonymous
                    else if (OneApi.authenticateAnonymous(method, path)) {
                        allowed = true;
                    }
                }

                if (allowed) {
                    //count(api.path);

                    return  new PQL(api.sentences, dh)
                                .place(params)
                                .signIn(userId, userName, role, info)
                                .placeParameters(request.getParameterMap())
                                .setHttpRequest(request)
                                .place(api.defaultValue)
                                .run();
                } else {
                    return "{\"error\": \"Access denied\"}";
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

    public OneApiRequester withJsonParameters() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        StringBuilder sb = new StringBuilder();
        if (attributes != null) {
            try {
                // body stream
                BufferedReader br = new BufferedReader(new InputStreamReader(attributes.getRequest().getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (sb.length() > 0) {
            this.params.putAll(new Json(sb.toString()).parseJavaMap("/"));
        }

        return  this;
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
        if (!Setting.OneApiMySQLConnection.isEmpty()) {
            TRAFFIC.add(path + DateTime.now().getString(",yyyy-MM-dd,HH"));
            if (TRAFFIC.size() >= 1000 || LastSaveTime.earlier(DateTime.now()) >= 60000) {
                DataAccess ds = new DataAccess(Setting.OneApiMySQLConnection);
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
