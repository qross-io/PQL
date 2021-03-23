package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.TypeExt;
import io.qross.fs.*;
import io.qross.jdbc.DataAccess;
import io.qross.jdbc.JDBC;
import io.qross.net.Redis;
import io.qross.security.Token;
import io.qross.time.DateTime;

import java.io.File;
import java.util.*;

public class OneApi {

    public String defaultValue = "";
    public String sentences = "";

    // path -> METHOD -> OneApi
    public static Map<String, Map<String, OneApi>> ALL = new HashMap<>();

    // path
    public static Set<String> OPEN = new HashSet<>();
    // path -> role or name
    public static Map<String, Set<String>> PERMIT = new HashMap<>();
    // secret mode :  token -> secret and update_time
    public static Map<String, String> SECRET = new HashMap<>();
    public static Map<String, SecretKey> KEYS = new HashMap<>();

    //read all api settings from resources:/api/
    public static void readAll() {

        //从jar包resources目录加载接口数据
        if (!Setting.OneApiResourceDirs.isEmpty()) {
            String[] dirs = Setting.OneApiResourceDirs.split(";");
            for (String dir : dirs) {
                String[] files = ResourceDir.open(dir).listFiles("*.sql");
                for (String file: files) {
                    readAPIs(file.substring(0, file.lastIndexOf(".")).substring(file.indexOf(dir)), ResourceFile.open(file).content().split("##"));
                }
            }
        }

        //load tokens
        if (!Setting.OneApiTokenList.isEmpty()) {
            String[] pairs = Setting.OneApiTokenList.split(";");
            for (String pair : pairs) {
                if (pair.indexOf("=") > 0) {
                    Token.TOKENS.put(pair.substring(pair.indexOf("=") + 1), pair.substring(0, pair.indexOf("=")));
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
        // methods:path=names
        if (!Setting.OneApiAccessPermit.isEmpty()) {
            String[] pairs = Setting.OneApiAccessPermit.split(";");
            for (String pair : pairs) {
                if (pair.indexOf("=") > 0) {
                    String path = pair.substring(0, pair.indexOf("="));
                    String[] names = pair.substring(pair.indexOf("=") + 1).split(",");
                    if (path.contains(":")) {
                        String[] methods = path.substring(0, path.indexOf(":")).split(",");
                        String[] bases = path.substring(path.indexOf(":") + 1).split(",");
                        for (String method : methods) {
                            for (String base : bases) {
                                String req = method.trim().toUpperCase() + ":" + base.trim();
                                if (!PERMIT.containsKey(req)) {
                                    PERMIT.put(req, new HashSet<>());
                                }
                                Collections.addAll(PERMIT.get(req), names);
                            }
                        }
                    }
                    else {
                        String[] bases = path.split(",");
                        for (String base : bases) {
                            if (!PERMIT.containsKey(base)) {
                                PERMIT.put(base, new HashSet<>());
                            }
                            Collections.addAll(PERMIT.get(base), names);
                        }
                    }
                }
            }
        }
        else {
            PERMIT.put("*", new HashSet<String>(){{ add("*"); }});
        }

        // load all api and config from database
        if (!Setting.OneApiServiceName.isEmpty()) {
            if (JDBC.hasQrossSystem()) {
                DataAccess ds = DataAccess.QROSS();
                int serviceId = 0;

                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_services'")) {
                    DataRow service = ds.executeDataRow("SELECT id, security_control FROM qross_api_services WHERE service_name=?", Setting.OneApiServiceName);
                    serviceId = service.getInt("id");
                    Setting.OneApiSecurityMode = service.getString("security_control");
                }

                if (Setting.OneApiSecurityMode.equals("none")) {
                    OPEN.add("*");
                    PERMIT.put("*", new HashSet<String>() {{ add("*"); }});
                }

                //load requester and token
                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_requesters'")) {
                    DataTable tokens = ds.executeDataTable("SELECT name, token FROM qross_api_requesters");
                    for (DataRow token : tokens.getRowList()) {
                        Token.TOKENS.put(token.getString("token"), token.getString("name"));
                    }
                    tokens.clear();
                }

                //load permit and api
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
                ds.close();
            }
        }
    }

    private static void readAPIs(String path, String[] APIs) {
        for (int i = 0; i < APIs.length; i++) {
            APIs[i] = APIs[i].trim();
            if (!APIs[i].isEmpty() && APIs[i].contains(TextFile.TERMINATOR())) {
                //it must be a new line after interface header
                String[] API = APIs[i].substring(0, APIs[i].indexOf(TextFile.TERMINATOR())).trim().split("\\|");
                //0 name
                //1 method
                //2 allowed | default values
                //3 allowed | default values

                if (API.length >= 1) {
                    OneApi api = new OneApi();
                    api.sentences = APIs[i].substring(APIs[i].indexOf(TextFile.TERMINATOR())).trim();

                    String url = path + "/" + API[0].trim();
                    String METHOD = "GET";
                    String permit = "";

                    if (API.length == 2) {
                        METHOD = API[1];
                    }
                    else if (API.length == 3) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                        }
                        else {
                            permit = API[2].trim();
                        }
                    }
                    else if (API.length == 4) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                            permit = API[3].trim();
                        }
                        else {
                            permit = API[2].trim();
                            api.defaultValue = API[3].trim();
                        }
                    }

                    if (!ALL.containsKey(url)) {
                        ALL.put(url, new LinkedHashMap<>());
                    }

                    String[] methods = METHOD.split(",");
                    String[] names = permit.isEmpty() ? new String[0] : permit.split(",");
                    for (String method : methods) {
                        ALL.get(url).put(method.trim(), api);

                        if (names.length > 0) {
                            String p = method.trim() + ":" + url;
                            if (!PERMIT.containsKey(p)) {
                                PERMIT.put(p, new HashSet<>());
                            }
                            for (String name : names) {
                                if (!name.trim().isEmpty()) {
                                    PERMIT.get(p).add(name.trim());
                                }
                            }
                        }
                    }
                    //PERMIT.get(permit).addAll(Arrays.asList(API[2].trim().split(",")));
                    //Collections.addAll(PERMIT.get(permit), API[3].trim().split(","));
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

    public static String getSecretKey(String token) {
        String key;
        if (io.qross.setting.Properties.contains("redis.qross.host")) {
            Redis redis = new Redis("qross");
            Object secret = redis.run("hget oneapi_secret_key " + token);
            if (secret == null) {
                key = getToken(Setting.OneApiSecretKeyDigit);
                redis.run("hset oneapi_secret_key " + token + " " + key);
                redis.run("set oneapi_secret_" + key + " " + token + " ex " + Setting.OneApiSecretKeyTTL);
            }
            else {
                key = secret.toString();
                if ((long) redis.run("exists oneapi_secret_" + key) == 1) {
                    //延长时间
                    redis.run("expire oneapi_secret_" + key + " " + Setting.OneApiSecretKeyTTL);
                }
                else {
                    key = getToken(Setting.OneApiSecretKeyDigit);
                    redis.run("hset oneapi_secret_key " + token + " " + key);
                    redis.run("set oneapi_secret_" + key + " " + token + " ex " + Setting.OneApiSecretKeyTTL);
                }
            }
            redis.close();
        }
        else {
            if (!SECRET.containsKey(token)) {
                key = getToken(Setting.OneApiSecretKeyDigit);
                SECRET.put(token, key);
            }
            else {
                key = SECRET.get(token);
                //过期则更新Key
                if (KEYS.get(key).expired()) {
                    KEYS.remove(key);
                    key = getToken(Setting.OneApiSecretKeyDigit);
                    SECRET.put(token, key);
                }
            }
            KEYS.put(key, new SecretKey(token));
        }

        return key;
    }

    //获取SecretKey的剩余存活时间负数表示不存在或已过期
    public static long ttlSecretKey(String key) {
        if (KEYS.containsKey(key)) {
            return KEYS.get(key).getTTL();
        }
        else {
            return -1;
        }
    }

    //验证token访问
    public static boolean authenticateToken(String method, String path, String token) {
        if (Token.TOKENS.containsKey(token)) {
            String requester = Token.TOKENS.get(token);
            String request = method.toUpperCase() + ":" + path;
            if (PERMIT.containsKey("*")) {
                if (PERMIT.get("*").contains("*") || PERMIT.get("*").contains(requester)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(path)) {
                if (PERMIT.get(path).contains("*") || PERMIT.get(path).contains(requester)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(request)) {
                if (PERMIT.get(request).contains("*") || PERMIT.get(request).contains(requester)) {
                    return true;
                }
            }
            else {
                String glob = path.substring(0, path.lastIndexOf("/"));
                while (!glob.isEmpty()) {
                    String base = glob + "/*";
                    String req = method.toUpperCase() + ":" + base;
                    if (PERMIT.containsKey(base)) {
                        if (PERMIT.get(base).contains("*") || PERMIT.get(base).contains(requester)) {
                            return true;
                        }
                    }
                    else if (PERMIT.containsKey(req)) {
                        if (PERMIT.get(req).contains("*") || PERMIT.get(req).contains(requester)) {
                            return true;
                        }
                    }
                    glob = glob.substring(0, glob.lastIndexOf("/"));
                }
            }
        }

        return false;
    }

    //验证Secret访问
    public static boolean authenticateSecretKey(String method, String path, String secretKey) {

        String token = null;
        if (io.qross.setting.Properties.contains("redis.qross.host")) {
            Redis redis = new Redis("qross");
            token = redis.command("get oneapi_secret_" + secretKey).asText();
            redis.close();
        }
        else {
            if (KEYS.containsKey(secretKey)) {
                SecretKey key = KEYS.get(secretKey);
                if (!key.expired()) {
                    token = key.getToken();
                }
            }
        }

        if (token != null && !token.isEmpty()) {
            //逻辑与Token逻辑相同
            String requester = Token.TOKENS.get(token);
            String request = method.toUpperCase() + ":" + path;
            if (PERMIT.containsKey("*")) {
                if (PERMIT.get("*").contains("*") || PERMIT.get("*").contains(requester)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(path)) {
                if (PERMIT.get(path).contains("*") || PERMIT.get(path).contains(requester)) {
                    return true;
                }
            }
            else if (PERMIT.containsKey(request)) {
                if (PERMIT.get(request).contains("*") || PERMIT.get(request).contains(requester)) {
                    return true;
                }
            }
            else {
                String glob = path.substring(0, path.lastIndexOf("/"));
                while (!glob.isEmpty()) {
                    String base = glob + "/*";
                    String req = method.toUpperCase() + ":" + base;
                    if (PERMIT.containsKey(base)) {
                        if (PERMIT.get(base).contains("*") || PERMIT.get(base).contains(requester)) {
                            return true;
                        }
                    }
                    else if (PERMIT.containsKey(req)) {
                        if (PERMIT.get(req).contains("*") || PERMIT.get(req).contains(requester)) {
                            return true;
                        }
                    }
                    glob = glob.substring(0, glob.lastIndexOf("/"));
                }
            }
        }

        return false;
    }

    //验证用户访问
    public static boolean authenticateUser(String method, String path, String username, String role) {
        String roleName = role;
        if (!role.startsWith("@")) {
            roleName = "@" + role;
        }
        String request = method.toUpperCase() + ":" + path;

        if (PERMIT.containsKey("*")) {
            if (PERMIT.get("*").contains("*") || PERMIT.get("*").contains(roleName) || PERMIT.get("*").contains(username)) {
                return true;
            }
        }
        else if (PERMIT.containsKey(path)) {
            if (PERMIT.get(path).contains("*") || PERMIT.get(path).contains(roleName) || PERMIT.get("*").contains(username)) {
                return true;
            }
        }
        else if (PERMIT.containsKey(request)) {
            if (PERMIT.get(request).contains("*") || PERMIT.get(request).contains(roleName) || PERMIT.get(request).contains(username)) {
                return true;
            }
        }
        else {
            String glob = path.substring(0, path.lastIndexOf("/"));
            while (!glob.isEmpty()) {
                String base = glob + "/*";
                String req = method.toUpperCase() + ":" + base;
                if (PERMIT.containsKey(base)) {
                    if (PERMIT.get(base).contains("*") || PERMIT.get(base).contains(roleName) || PERMIT.get(base).contains(username)) {
                        return true;
                    }
                }
                else if (PERMIT.containsKey(req)) {
                    if (PERMIT.get(req).contains("*") || PERMIT.get(req).contains(roleName) || PERMIT.get(req).contains(username)) {
                        return true;
                    }
                }
                glob = glob.substring(0, glob.lastIndexOf("/"));
            }
        }

        return false;
    }

    //验证匿名访问
    public static boolean authenticateAnonymous(String method, String path) {
        String request = method.toUpperCase() + ":" + path;
        //OPEN 为空不可匿名访问
        if (OPEN.isEmpty()) {
            return false;
        }
        else if (OPEN.contains("*") || OPEN.contains(path) || OPEN.contains(request)) {
            return true;
        }
        else {
            String glob = path.substring(0, path.lastIndexOf("/"));
            while (!glob.isEmpty()) {
                String base = glob + "/*";
                if (OPEN.contains(base) || OPEN.contains(method.toUpperCase() + ":" + base)) {
                    return true;
                }
                glob = glob.substring(0, glob.lastIndexOf("/"));
            }
            return false;
        }
    }

    public static boolean authenticateManagementKey(String key) {
        return (key.equals(Setting.OneApiManagementKey));
    }

    public static OneApi pick(String path, String method) {
        return ALL.get(path).get(method.toUpperCase());
    }

    public static Map<String, OneApi> pick(String path) {
        return ALL.get(path);
    }

    public static String get(String path, String method) {
        OneApi api = pick(path, method);
        if (api != null) {
            return api.sentences;
        }
        else {
            return null;
        }
    }

    public static Map<String, String> get(String path) {
        Map<String, OneApi> api = pick(path);
        if (api != null) {
            Map<String, String> sentences = new HashMap<>();
            for (String method : api.keySet()) {
                sentences.put(method, api.get(method).sentences);
            }
            return sentences;
        }
        else {
            return null;
        }
    }

    //path and method of all api
    public static Map<String, String> getAll() {
        if (ALL.isEmpty()) {
            readAll();
        }

        Map<String, String> keys = new HashMap<>();
        for (String key : ALL.keySet()) {
            keys.put(key, String.join(",", ALL.get(key).keySet()));
        }
        return keys;
    }

    //refresh all api
    public static int refresh() {
        OneApi.ALL.clear();
        OneApi.readAll();
        return OneApi.ALL.size();
    }

    public static int count() {
        int count = 0;
        for (String path : OneApi.ALL.keySet()) {
            count += OneApi.ALL.get(path).size();
        }

        return count;
    }

    public static String getSetting(String key) {
        return io.qross.setting.Properties.get(key, null);
    }

    public static Map<String, Object> getSettings() {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("oneapi.service.name", Setting.OneApiServiceName);
        settings.put("oneapi.security.mode", Setting.OneApiSecurityMode);
        settings.put("oneapi.resources.dirs", Setting.OneApiResourceDirs);
        settings.put("oneapi.access.open", Setting.OneApiAccessOpen);
        settings.put("oneapi.token.list", Setting.OneApiTokenList);
        settings.put("oneapi.access.permit", Setting.OneApiAccessPermit);
        settings.put("oneapi.secret.key.ttl", Setting.OneApiSecretKeyTTL);
        settings.put("oneapi.secret.key.digit", Setting.OneApiSecretKeyDigit);
        settings.put("oneapi.management.key", Setting.OneApiManagementKey);

        return settings;
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
        return new OneApiRequester().request(path);
    }

    public static Object request(String path, String connectionName) {
        return new OneApiRequester().request(path, connectionName);
    }

    public static Object request(String path, DataHub dh){
        return new OneApiRequester().request(path, dh);
    }
}

class SecretKey {
    public String token;
    public DateTime createTime;

    public SecretKey(String token) {
        this.token = token;
        createTime = DateTime.now();
    }

    public boolean expired() {
        return createTime.earlier(DateTime.now()) / 1000 > Setting.OneApiSecretKeyTTL;
    }

    public String getToken() {
        return token;
    }

    public long getTTL() {
        return Setting.OneApiSecretKeyTTL - createTime.earlier(DateTime.now()) / 1000;
    }
}