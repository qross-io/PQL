package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.fs.Directory;
import io.qross.fs.FileReader;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.jdbc.DataAccess;
import io.qross.net.Json;
import io.qross.pql.PQL;
import io.qross.setting.Properties;
import io.qross.time.DateTime;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;

public class OneApi {

    public String path = "";
    public String defaultValue = "";
    public String sentences = "";
    public Set<String> allowed = new HashSet<>();

    // path -> METHOD -> OneApi
    public static Map<String, Map<String, OneApi>> ALL = new HashMap<>();
    // token -> allowed name
    public static Map<String, String> TOKENS = new HashMap<>();

    //service,path,date,hour -> pv
    public static List<String> TRAFFIC = new ArrayList<>();
    private static DateTime LastSaveTime = DateTime.now();

    //read all api settings from resources:/api/
    public static void readAll() {
        ALL.clear();

        //从jar包resources目录加载接口数据
        if (!Setting.OneApiResourceDirs.isEmpty()) {
            String[] dirs = Setting.OneApiResourceDirs.split(";");
            for (String dir : dirs) {
                String[] files = ResourceDir.open(dir).listFiles("*.sql");
                for (String file: files) {
                    readAPIs(file.substring(file.indexOf(dir) + dir.length(), file.lastIndexOf(".")), ResourceFile.open(file).content().split("##"));
                }
            }
        }

        //load api from external dirs out of jar
        if (!Setting.OneApiExternalDirs.isEmpty()) {
            String[] dirs = Setting.OneApiExternalDirs.split(";");
            for (String dir : dirs) {
                File[] files = Directory.listFiles(dir, "*.sql", true);
                for (File file: files) {
                    String path = file.getPath();
                    readAPIs(path.substring(dir.length(), path.lastIndexOf(".")), new FileReader(file).readToEnd().split("##"));
                }
            }
        }

        //从数据表加载接口数据和TOKEN
        if (!Setting.OneApiMySQLConnection.isEmpty()) {
            DataAccess ds = new DataAccess(Setting.OneApiMySQLConnection);

            if (!Setting.OneApiServiceName.isEmpty()) {

                DataTable APIs = ds.executeDataTable("SELECT * FROM qross_api_in_one WHERE service_name=?", Setting.OneApiServiceName);
                for (DataRow API : APIs.getRowList()) {
                    OneApi api = new OneApi();
                    api.path = API.getString("path");
                    api.sentences = API.getString("pql");
                    api.defaultValue = API.getString("default_value");

                    if (!ALL.containsKey(api.path)) {
                        ALL.put(api.path, new LinkedHashMap<>());
                    }
                    String[] methods = API.getString("method").toUpperCase().split(",");
                    for (String method : methods) {
                        ALL.get(api.path).put(method.trim(), api);
                    }
                }
                APIs.clear();
            }

            DataTable tokens = ds.executeDataTable("SELECT name, token FROM qross_api_tokens");
            for (DataRow token : tokens.getRowList()) {
                TOKENS.put(token.getString("token"), token.getString("name"));
            }
            tokens.clear();
            ds.close();
        }
    }

    private static void readAPIs(String path, String[] APIs) {
        for (int i = 0; i < APIs.length; i++) {
            APIs[i] = APIs[i].trim();
            if (!APIs[i].isEmpty()) {
                String[] API = APIs[i].split("\\|", -1);
                //0 name
                //1 method
                //2 allowed | default values
                //3 allowed | default values
                //4 PQL
                if (API.length >= 2) {
                    OneApi api = new OneApi();
                    if (api.path.endsWith("/")) {
                        api.path = path + API[0].trim();
                    }
                    else {
                        api.path = path + "/" + API[0].trim();
                    }

                    String METHOD = "GET";
                    if (API.length == 2) {
                        api.sentences = API[1].trim();
                    }
                    else if (API.length == 3) {
                        if (API[1].contains("=")) {
                            api.defaultValue = API[1];
                        }
                        else {
                            METHOD = API[1];
                        }
                        api.sentences = API[2].trim();
                    }
                    else if (API.length == 4) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                        }
                        else {
                            api.allowed.addAll(Arrays.asList(API[2].trim().split(",")));
                        }
                        api.sentences = API[3].trim();
                    }
                    else if (API.length == 5) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                            api.allowed.addAll(Arrays.asList(API[3].trim().split(",")));
                        }
                        else {
                            api.allowed.addAll(Arrays.asList(API[2].trim().split(",")));
                            api.defaultValue = API[3].trim();
                        }
                        api.sentences = API[4].trim();
                    }

                    if (!ALL.containsKey(api.path)) {
                        ALL.put(api.path, new LinkedHashMap<>());
                    }

                    String[] methods = METHOD.split(",");
                    for (String method : methods) {
                        ALL.get(api.path).put(method.trim(), api);
                    }
                }
            }
        }
    }

    //refresh all api
    public static int refresh() {
        OneApi.readAll();
        return OneApi.ALL.size();
    }

    //path and method of all api
    public static Map<String, String> all() {
        if (ALL.isEmpty()) {
            readAll();
        }

        Map<String, String> keys = new HashMap<>();
        for (String key : ALL.keySet()) {
            keys.put(key, String.join(",", ALL.get(key).keySet()));
        }
        return keys;
    }

    //unsaved traffic data
    public static List<String> traffic() {
        return TRAFFIC;
    }

    public static List<String> listResources(String path) {
        List<String> files = new ArrayList<>();
        if (!path.isEmpty()) {
            String[] dirs = path.split(";");
            for (String dir : dirs) {
                files.addAll(Arrays.asList(ResourceDir.open(dir).listFiles("*.sql")));
            }
        }
        return files;
    }

    public static List<String> listDirectory(String path) {
        List<String> files = new ArrayList<>();
        //load api from external dirs out of jar
        if (!path.isEmpty()) {
            String[] dirs = path.split(";");
            for (String dir : dirs) {
                for (File file : Directory.listFiles(dir, "*.sql", true)) {
                    files.add(file.getPath());
                }
            }
        }
        return files;
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
            }
        }
    }

    public static Object request(String path) {
        return request(path, DataHub.DEFAULT());
    }

    public static Object request(String path, String connectionName) { return request(path, new DataHub(connectionName)); }

    public static Object request(String path, DataHub dh) {
        return request(path, new HashMap<String, String>(), dh);
    }

    public static Object requestWithJsonParameters(String path) {
        return requestWithJsonParameters(path, DataHub.DEFAULT());
    }

    public static Map<String, String> getJsonParameters() {
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
            return new Json(sb.toString()).parseJavaMap("/");
        }
        else {
            return new HashMap<>();
        }
    }

    public static Object requestWithJsonParameters(String path, DataHub dh) {
        return request(path, getJsonParameters(), dh);
    }

    public static Object request(String path, Map<String, String> params) {
        return request(path, params, DataHub.DEFAULT());
    }

    public static Object request(String path, Map<String, String> params, String connectionName) {
        return request(path, params, new DataHub(connectionName));
    }

    public static Object request(String path, Map<String, String> params, DataHub dh) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();

            String method = request.getMethod();
            if (!OneApi.contains(path, method)) {
                OneApi.readAll();
            }

            if (OneApi.contains(path, method)) {
                OneApi api = OneApi.pick(path, method);

                String security = Properties.get("oneapi.security.enabled", "0");
                if (security.equalsIgnoreCase("true") || security.equalsIgnoreCase("yes")) {
                    security = "1";
                }

                boolean allowed = false;
                if (security.equals("1")) {
                    String token = request.getParameter("token");
                    if (token == null || token.isEmpty()) {
                        for (Cookie cookie : request.getCookies()) {
                            if (cookie.getName().equalsIgnoreCase("token")) {
                                token = cookie.getValue();
                                break;
                            }
                        }
                    }

                    if (TOKENS.containsKey(token) && (api.allowed.isEmpty() || api.allowed.contains(TOKENS.get(token)))) {
                        allowed = true;
                    }
                } else {
                    allowed = true;
                }

                if (allowed) {
                    count(api.path);

                    return  new PQL(api.sentences, dh)
                            .place(params)
                            .placeParameters(request.getParameterMap())
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

    public static Object execute(String path) {
        return execute(path, DataHub.DEFAULT());
    }

    public static Object execute(String path, String connectionName) {
        return execute(path, new DataHub(connectionName));
    }

    public static Object execute(String path, DataHub dh) {
        if (!OneApi.contains(path, "GET")) {
            OneApi.readAll();
        }

        String params = "";
        if (path.contains("?")) {
            path = path.substring(0, path.indexOf("?"));
            params = path.substring(path.indexOf("?") + 1);
        }

        if (OneApi.contains(path, "GET")) {
            OneApi api = OneApi.pick(path, "GET");
            PQL PQL = new PQL(api.sentences, dh);

            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes != null) {
                HttpServletRequest request = attributes.getRequest();
                PQL.placeParameters(request.getParameterMap());
            }

            if (!params.isEmpty()) {
                PQL.place(params);
            }

            return PQL.place(api.defaultValue).run();
        }
        else {
            return "{\"error\": \"WRONG or MISS path '" + path + "'\"}";
        }
    }

    public static boolean contains(String path) {
        return ALL.containsKey(path);
    }

    public static boolean contains(String path, String method) {
        return ALL.containsKey(path) && ALL.get(path).containsKey(method.toUpperCase());
    }

    public static OneApi pick(String path, String method) {
        return ALL.get(path).get(method.toUpperCase());
    }
}
