package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.ext.TypeExt;
import io.qross.fs.*;
import io.qross.jdbc.DataAccess;
import io.qross.jdbc.JDBC;
import io.qross.net.Cookies;
import io.qross.net.Redis;
import io.qross.security.Token;
import io.qross.setting.Global;
import io.qross.time.DateTime;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OneApi {

    public static int SERVICE_ID = 0;
    public static int TRAFFIC_GRADE = 0;
    // path -> METHOD -> OneApi
    public static Map<String, Map<String, OneApiPlain>> ALL = new HashMap<>();

    // path
    public static Set<String> OPEN = new HashSet<>();
    // path -> role or name
    public static Map<String, Set<String>> PERMIT = new HashMap<>();
    // secret mode :  token -> secret and update_time
    public static Map<String, String> SECRET = new HashMap<>();
    public static Map<String, OneApiSecretKey> KEYS = new HashMap<>();

    //read all api settings from resources:/api/ and qross database
    public static void readAll() {

        //从jar包resources目录加载接口数据
        if (!Setting.OneApiResourceDirs.isEmpty()) {
            String[] dirs = Setting.OneApiResourceDirs.split(";");
            for (String dir : dirs) {
                String[] files = ResourceDir.open(dir).listFiles("*.sql");
                for (String file: files) {
                    readAPIs(file.substring(0, file.lastIndexOf(".")).substring(file.indexOf(dir)), ResourceFile.open(file).content());
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

                if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_services'")) {
                    DataRow service = ds.executeDataRow("SELECT id, security_control, traffic_grade FROM qross_api_services WHERE service_name=?", Setting.OneApiServiceName);
                    OneApi.SERVICE_ID = service.getInt("id");
                    OneApi.TRAFFIC_GRADE = service.getInt("traffic_grade");
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
                    //load permit
                    if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_requesters_allows'")) {
                        DataTable controls = ds.executeDataTable("SELECT A.control, A.requester_id, B.name FROM qross_api_requesters_allows A INNER JOIN qross_api_requesters B ON (A.service_id=?) AND A.requester_id=B.id", OneApi.SERVICE_ID);
                        for (DataRow control : controls.getRowList()) {
                            String allow = control.getString("control");
                            if (PERMIT.containsKey(allow)) {
                                PERMIT.put(allow, new HashSet<>());
                            }
                            PERMIT.get(allow).add(control.getString("name"));
                        }
                    }
                }

                //load api
                if (OneApi.SERVICE_ID > 0) {
                    if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_in_one'")) {
                        //synchronize to database
                        String updateTime = DateTime.now().getString("yyyy-MM-dd HH:mm:ss");
                        ds.setBatchCommand("INSERT INTO qross_api_in_one (service_id, method, path, pql, default_values, `source`, title, description, params, allowed, permit, return_value, creator, mender, update_time) VALUES (?, ?, ?, ?, ?, 'file', ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE pql=?, default_values=?, title=?, description=?, params=?, allowed=?, permit=?, return_value=?, creator=?, mender=?, update_time=?");
                        for (String path : ALL.keySet()) {
                            for (Map.Entry<String, OneApiPlain> plain : ALL.get(path).entrySet()) {
                                OneApiPlain api = plain.getValue();
                                ds.addBatch(OneApi.SERVICE_ID, plain.getKey(), path, api.statement, api.defaultValues, api.title, api.description, api.params, api.allowed, api.permit, api.returnValue, api.creator, api.mender, updateTime, api.statement, api.defaultValues, api.title, api.description, api.params, api.allowed, api.permit, api.returnValue, api.creator, api.mender, updateTime);
                            }
                        }
                        ds.executeBatchUpdate();
                        //delete deprecated
                        ds.executeNonQuery("DELETE FROM qross_api_in_one WHERE service_id=? AND `source`='file' AND update_time<>? AND TIMESTAMPDIFF(DAY, update_time, NOW()) > 30", OneApi.SERVICE_ID, updateTime);

                        //renew all api
                        DataTable APIs = ds.executeDataTable("SELECT id, path, method, pql, default_values, IF(return_value_example IS NULL OR return_value_example = '', 1, 0) AS exampled FROM qross_api_in_one WHERE service_id=?", OneApi.SERVICE_ID);
                        for (DataRow API : APIs.getRowList()) {
                             String path = API.getString("path");
                            if (!ALL.containsKey(path)) {
                                ALL.put(path, new LinkedHashMap<>());
                            }
                            ALL.get(path).put(API.getString("method").toUpperCase(), new OneApiPlain(API.getInt("id"), API.getString("pql"), API.getString("default_values"), API.getBoolean("exampled")));
                        }
                        APIs.clear();
                    }
                }
                ds.close();
            }
        }
    }

    public static void readAPIs(String path, String content) {

        Pattern p = Pattern.compile("^\\s*##\\s*([a-z0-9_-]+)\\b", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
        Matcher m = p.matcher(content);
        List<String> splitted = new ArrayList<>();
        while (m.find()) {
            String label = m.group(0);
            String prefix = TypeExt.StringExt(content).takeBefore(label).trim();
            String comment = "";
            if (prefix.endsWith("*/") && prefix.contains("/*")) {
                comment = TypeExt.StringExt(prefix).takeAfterLast("/*");
                comment = comment.substring(0, comment.length() - 2).trim();
                prefix = TypeExt.StringExt(prefix).takeBeforeLast("/*");
            }
            splitted.add(prefix);
            splitted.add(comment);
            splitted.add(m.group(1));

            content = TypeExt.StringExt(content).takeAfter(label).trim();
        }
        splitted.add(content);

        for (int i = 1; i < splitted.size(); i += 3) {

            OneApiPlain api = new OneApiPlain();

            if (!splitted.get(i).isEmpty()) {
                String[] comment = splitted.get(i).split(TextFile.TERMINATOR());
                for (String line : comment) {
                    line = line.replaceAll("^\\s*\\*\\s*", "");

                    if (line.startsWith("#")) {
                        line = line.substring(1);
                        if (!api.params.isEmpty()) {
                            api.params += "&";
                        }
                        if (line.contains(" ")) {
                            api.params += line.substring(0, line.indexOf(" ")) + "=" + line.substring(line.indexOf(" ") + 1).trim().replace("&", "%26").replace("=", "%3D").replace(" ", "%20");
                        } else {
                            api.params += line + "=";
                        }

                        if (api.params.length() > 2000) {
                            api.params = api.params.substring(0, 2000);
                        }
                    } else if (line.startsWith("@")) {
                        if (line.startsWith("@return")) {
                            api.returnValue = line.substring(7).trim();
                        } else if (line.startsWith("@created") || line.startsWith("@create")) {
                            api.creator = line.substring(8);
                        } else if (line.startsWith("@updated") || line.startsWith("@update")) {
                            api.mender = line.substring(8);
                        } else if (line.startsWith("@permit")) {
                            api.permit = line.substring(7).trim();
                        }
                    } else {
                        if (api.title.isEmpty()) {
                            api.title = line;
                        } else {
                            if (api.description.isEmpty()) {
                                api.description = line;
                            } else {
                                api.description += "<br/>" + line;
                            }
                            if (api.description.length() > 500) {
                                api.description = api.description.substring(0, 500);
                            }
                        }
                    }
                }
            }

            String url = path + "/" + splitted.get(i+1);
            String body = "name";
            if (i + 2 < splitted.size()) {
                body += splitted.get(i + 2);
            }
            if (body.contains("\n")) {
                api.statement = body.substring(body.indexOf("\n") + 1);
                body = body.substring(0, body.indexOf("\n"));
            }
            //it must be a new line after interface header
            String[] options =  body.split("\\|");
            if (options.length >= 1) {

                String METHOD = "GET";

                if (options.length == 2) {
                    METHOD = options[1].trim().toUpperCase();
                }
                else if (options.length == 3) {
                    METHOD = options[1].trim().toUpperCase();
                    if (options[2].contains("=")) {
                        api.defaultValues = options[2].trim();
                    }
                    else {
                        api.allowed = options[2].trim();
                    }
                }
                else if (options.length == 4) {
                    METHOD = options[1].trim().toUpperCase();
                    if (options[2].contains("=")) {
                        api.defaultValues = options[2].trim();
                        api.allowed = options[3].trim();
                    }
                    else {
                        api.allowed = options[2].trim();
                        api.defaultValues = options[3].trim();
                    }
                }

                if (!ALL.containsKey(url)) {
                    ALL.put(url, new LinkedHashMap<>());
                }

                String[] names = api.allowed.isEmpty() ? new String[0] : api.allowed.split(",");
                ALL.get(url).put(METHOD, api);

                if (names.length > 0) {
                    String p1 = METHOD + ":" + url;
                    if (!PERMIT.containsKey(p1)) {
                        PERMIT.put(p1, new HashSet<>());
                    }
                    for (String name : names) {
                        if (!name.trim().isEmpty()) {
                            PERMIT.get(p1).add(name.trim());
                        }
                    }
                }

                //PERMIT.get(permit).addAll(Arrays.asList(API[2].trim().split(",")));
                //Collections.addAll(PERMIT.get(permit), API[3].trim().split(","));
            }

        }
    }

    public static void saveExample(int id, Object example) {
        if (JDBC.hasQrossSystem()) {
            DataAccess ds = DataAccess.QROSS();
            ds.executeNonQuery("UPDATE qross_api_in_one SET return_value_example=? WHERE id=?", example, id);
            ds.close();
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
            KEYS.put(key, new OneApiSecretKey(token));
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
                OneApiSecretKey key = KEYS.get(secretKey);
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

    public static OneApiPlain pick(String path, String method) {
        return ALL.get(path).get(method.toUpperCase());
    }

    public static Map<String, OneApiPlain> pick(String path) {
        return ALL.get(path);
    }

    public static String get(String path, String method) {
        OneApiPlain api = pick(path, method);
        if (api != null) {
            return api.statement;
        }
        else {
            return null;
        }
    }

    public static Map<String, String> get(String path) {
        Map<String, OneApiPlain> api = pick(path);
        if (api != null) {
            Map<String, String> sentences = new HashMap<>();
            for (String method : api.keySet()) {
                sentences.put(method, api.get(method).statement);
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

    public static void resetReturnValueExample(int id) {
        if (JDBC.hasQrossSystem()) {
            DataAccess ds = DataAccess.QROSS();
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_api_in_one'")) {
                ds.executeNonQuery("UPDATE qross_api_in_one SET return_value_example='' WHERE id=?", id);
                DataRow row = ds.executeDataRow("SELECT path, method FROM qross_api_in_one WHERE id=?", id);
                String path = row.getString("path");
                String method = row.getString("method");
                if (ALL.containsKey(path) && ALL.get(path).containsKey(method)) {
                    ALL.get(path).get(method).exampled = true;
                }
            }
            ds.close();
        }
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