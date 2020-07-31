package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.TypeExt;
import io.qross.fs.*;
import io.qross.jdbc.DataAccess;

import java.io.File;
import java.util.*;

public class OneApi {

    public String defaultValue = "";
    public String sentences = "";

    // path -> METHOD -> OneApi
    public static Map<String, Map<String, OneApi>> ALL = new HashMap<>();
    // token -> allowed name
    public static Map<String, String> TOKENS = new HashMap<>();
    // path
    public static Set<String> OPEN = new HashSet<>();
    // path - name
    public static Map<String, Set<String>> PERMIT = new HashMap<>();

    //read all api settings from resources:/api/
    public static void readAll() {

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

        //load tokens
        if (!Setting.OneApiTokenList.isEmpty()) {
            String[] pairs = Setting.OneApiTokenList.split(";");
            for (String pair : pairs) {
                if (pair.indexOf("=") > 0) {
                    TOKENS.put(pair.substring(pair.indexOf("=") + 1), pair.substring(0, pair.indexOf("=")));
                }
            }
        }

        // *
        if (!Setting.OneApiAccessOpen.isEmpty()) {
            String[] paths = Setting.OneApiAccessOpen.split(";");
            for (String path : paths) {
                if (path.contains(":")) {
                    String[] methods = path.substring(0, path.indexOf(":")).split(",");
                    String base = path.substring(path.indexOf(":") + 1);
                    for (String method : methods) {
                        OPEN.add(method + ":" + base);
                    }
                }
                else if (!path.equals("")) {
                    OPEN.add(path);
                }
            }
        }

        // *=* means allow all
        if (!Setting.OneApiAccessPermit.isEmpty()) {
            String[] pairs = Setting.OneApiAccessPermit.split(";");
            for (String pair : pairs) {
                if (pair.indexOf("=") > 0) {
                    String path = pair.substring(pair.indexOf("=") + 1);
                    String[] names = pair.substring(0, pair.indexOf("=")).split(",");
                    if (path.contains(":")) {
                        String[] methods = path.substring(0, path.indexOf(":")).split(",");
                        String base = path.substring(path.indexOf(":") + 1);
                        for (String method : methods) {
                            String req = method + ":" + base;
                            if (!PERMIT.containsKey(req)) {
                                PERMIT.put(req, new HashSet<>());
                            }
                            Collections.addAll(PERMIT.get(req), names);
                        }
                    }
                    else {
                        if (!PERMIT.containsKey(path)) {
                            PERMIT.put(path, new HashSet<>());
                        }
                        Collections.addAll(PERMIT.get(path), names);
                    }
                }
            }
        }

        // load all api and config from database
        if (!Setting.OneApiMySQLConnection.isEmpty()) {
            DataAccess ds = new DataAccess(Setting.OneApiMySQLConnection);

            if (!Setting.OneApiServiceName.isEmpty()) {
                int serviceId = 0;
                String security = "token";

                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_services'")) {
                    DataRow service = ds.executeDataRow("SELECT id, security_control FROM qross_services WHERE service_name=?", Setting.OneApiServiceName);
                    serviceId = service.getInt("id");
                    security = service.getString("security_control");
                }

                if (security.equals("none")) {
                    OPEN.add("*");
                    PERMIT.put("*", new HashSet<String>() {{ add("*"); }});
                }

                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_requsters'")) {
                    DataTable tokens = ds.executeDataTable("SELECT name, token FROM qross_api_requsters");
                    for (DataRow token : tokens.getRowList()) {
                        TOKENS.put(token.getString("token"), token.getString("name"));
                    }
                    tokens.clear();
                }

                if (serviceId > 0) {
                    if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_in_one'")) {
                        DataTable controls = ds.executeDataTable("SELECT A.control, A.requester_id, B.name FROM qross_api_requesters_allows A INNER JOIN qross_api_requesters B ON (A.service_id=?) AND A.requester_id=B.id", serviceId);
                        for (DataRow control : controls.getRowList()) {
                            String allow = control.getString("control");
                            if (PERMIT.containsKey(allow)) {
                                PERMIT.put(allow, new HashSet<>());
                            }
                            PERMIT.get(allow).add(control.getString("name"));
                        }

                        DataTable APIs = ds.executeDataTable("SELECT * FROM qross_api_in_one WHERE service_id=?", serviceId);
                        for (DataRow API : APIs.getRowList()) {
                            OneApi api = new OneApi();
                            String path = API.getString("path");
                            api.sentences = API.getString("pql");
                            api.defaultValue = API.getString("default_value");

                            if (!ALL.containsKey(path)) {
                                ALL.put(path, new LinkedHashMap<>());
                            }
                            String[] methods = API.getString("method").toUpperCase().split(",");
                            for (String method : methods) {
                                ALL.get(path).put(method, api);
                            }
                        }
                        APIs.clear();
                    }
                }
            }

            ds.close();
        }
    }

    private static void readAPIs(String path, String[] APIs) {
        for (int i = 0; i < APIs.length; i++) {
            APIs[i] = APIs[i].trim();
            if (!APIs[i].isEmpty() && APIs[i].contains(TextFile.TERMINATOR())) {
                //单个接口中必须有换行
                String[] API = APIs[i].substring(0, APIs[i].indexOf(TextFile.TERMINATOR())).trim().split("\\|");
                //0 name
                //1 method
                //2 allowed | default values
                //3 allowed | default values

                if (API.length >= 1) {
                    OneApi api = new OneApi();
                    api.sentences = APIs[i].substring(APIs[i].indexOf(TextFile.TERMINATOR())).trim();
                    String url = "/" + path;
                    if (path.endsWith("/")) {
                        url += API[0].trim();
                    }
                    else {
                        url += "/" + API[0].trim();
                    }

                    String METHOD = "GET";
                    if (API.length == 2) {
                        if (API[1].contains("=")) {
                            api.defaultValue = API[1];
                        }
                        else {
                            METHOD = API[1];
                        }
                    }
                    else if (API.length == 3) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                        }
                        else {
                            if (!PERMIT.containsKey(url)) {
                                PERMIT.put(url, new HashSet<>());
                            }
                            PERMIT.get(url).addAll(Arrays.asList(API[2].trim().split(",")));
                        }
                    }
                    else if (API.length == 4) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                            if (!PERMIT.containsKey(url)) {
                                PERMIT.put(url, new HashSet<>());
                            }
                            Collections.addAll(PERMIT.get(url), API[3].trim().split(","));
                        }
                        else {
                            if (!PERMIT.containsKey(url)) {
                                PERMIT.put(url, new HashSet<>());
                            }
                            Collections.addAll(PERMIT.get(url), API[3].trim().split(","));
                            api.defaultValue = API[3].trim();
                        }
                    }

                    if (!ALL.containsKey(url)) {
                        ALL.put(url, new LinkedHashMap<>());
                    }

                    String[] methods = METHOD.split(",");
                    for (String method : methods) {
                        ALL.get(url).put(method.trim(), api);
                    }
                }
            }
        }
    }

    public static boolean contains(String path) {
        return ALL.containsKey(path);
    }

    public static boolean contains(String path, String method) {
        return ALL.containsKey(path) && ALL.get(path).containsKey(method.toUpperCase());
    }

    public static String getToken(int digit) {
        return TypeExt.StringExt("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890").shuffle(digit);
    }

    //验证token访问
    public static boolean authenticateToken(String method, String path, String token) {
        if (TOKENS.containsKey(token)) {
            String name = TOKENS.get(token);
            if (PERMIT.containsKey("*")) {
                if (PERMIT.get("*").contains("*") || PERMIT.get("*").contains(name)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(path)) {
                if (PERMIT.get(path).contains("*") || PERMIT.get(path).contains(name)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(method + ":" + path)) {
                if (PERMIT.get(method + ":" + path).contains("*") || PERMIT.get(method + ":" + path).contains(name)) {
                    return true;
                }
            }
            else {
                String glob = path.substring(0, path.lastIndexOf("/"));
                while (!glob.isEmpty()) {
                    String all = glob + "/*";
                    if (PERMIT.containsKey(all)) {
                        if (PERMIT.get(glob + "*").contains("*") || PERMIT.get(glob + "*").contains(name)) {
                            return true;
                        }
                    }
                    glob = glob.substring(0, path.lastIndexOf("/"));
                }
            }
        }

        return false;
    }

    public static boolean authenticateRole(String method, String path, String role) {
        if (!role.isEmpty()) {
            if (PERMIT.containsKey("*")) {
                if (PERMIT.get("*").contains("*") || PERMIT.get("*").contains(role)) {
                    return true;
                }
            } else if (PERMIT.containsKey(path)) {
                if (PERMIT.get(path).contains("*") || PERMIT.get(path).contains(role)) {
                    return true;
                }
            } else if (PERMIT.containsKey(method + ":" + path)) {
                if (PERMIT.get(method + ":" + path).contains("*") || PERMIT.get(method + ":" + path).contains(role)) {
                    return true;
                }
            } else {
                String glob = path.substring(0, path.lastIndexOf("/"));
                while (!glob.isEmpty()) {
                    String all = glob + "/*";
                    if (PERMIT.containsKey(all)) {
                        if (PERMIT.get(glob + "*").contains("*") || PERMIT.get(glob + "*").contains(role)) {
                            return true;
                        }
                    }
                    glob = glob.substring(0, path.lastIndexOf("/"));
                }
            }
        }

        return false;
    }

    //验证匿名访问
    public static boolean authenticateAnonymous(String method, String path) {
        //OPEN 为空不可匿名访问
        if (OPEN.isEmpty()) {
            return false;
        }
        else if (OPEN.contains("*") || OPEN.contains(path) || OPEN.contains(method + ":" + path)) {
            return true;
        }
        else {
            String glob = path.substring(0, path.lastIndexOf("/"));
            while (!glob.isEmpty()) {
                String all = glob + "/*";
                if (OPEN.contains(all) || OPEN.contains(method + ":" + all)) {
                    return true;
                }
                glob = glob.substring(0, path.lastIndexOf("/"));
            }
            return false;
        }
    }

    public static OneApi pick(String path, String method) {
        return ALL.get(path).get(method.toUpperCase());
    }

    //refresh all api
    public static int refresh() {
        OneApi.ALL.clear();
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

    public static Object requestWithJsonParameters(String path) {
        return new OneApiRequester(path).withJsonParameters().request();
    }

    public static Object requestWithJsonParameters(String path, DataHub dh) {
        return new OneApiRequester(path, dh).withJsonParameters().request();
    }

    public static Object requestWithJsonParameters(String path, String connectionName) {
        return new OneApiRequester(path, connectionName).withJsonParameters().request();
    }

    public static OneApiRequester withJsonParameters() {
        return new OneApiRequester().withJsonParameters();
    }

    public static OneApiRequester signIn(Map<String, Object> info) {
        return new OneApiRequester().signIn(info);
    }

    public static OneApiRequester signIn(int userId, String userName, String role) {
        return new OneApiRequester().signIn(userId, userName, role);
    }

    public static OneApiRequester signIn(int userId, String userName, String role, Map<String, Object> info) {
        return new OneApiRequester().signIn(userId, userName, role, info);
    }

    public static Object request(String path) {
        return new OneApiRequester(path).request();
    }

    public static Object request(String path, String connectionName) {
        return new OneApiRequester(path, connectionName).request();
    }

    public static Object request(String path, DataHub dh){
        return new OneApiRequester(path, dh).request();
    }

    public static Object request(String path, Map<String, String> params) {
        return new OneApiRequester(path, params).request();
    }

    public static Object request(String path, Map<String, String> params, String connectionName) {
        return new OneApiRequester(path, params, new DataHub(connectionName));
    }

    public static Object request(String path, Map<String, String> params, DataHub dh) {
        return new OneApiRequester(path, params, dh).request();
    }
}
