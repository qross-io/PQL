package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.fs.*;
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
import io.qross.ext.Console;
import scala.collection.immutable.Stream;

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
                    if (api.path.endsWith("/")) {
                        api.path = path + API[0].trim();
                    }
                    else {
                        api.path = path + "/" + API[0].trim();
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
                            api.allowed.addAll(Arrays.asList(API[2].trim().split(",")));
                        }
                    }
                    else if (API.length == 4) {
                        METHOD = API[1].trim().toUpperCase();
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                            api.allowed.addAll(Arrays.asList(API[3].trim().split(",")));
                        }
                        else {
                            api.allowed.addAll(Arrays.asList(API[2].trim().split(",")));
                            api.defaultValue = API[3].trim();
                        }
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

    public static boolean contains(String path) {
        return ALL.containsKey(path);
    }

    public static boolean contains(String path, String method) {
        return ALL.containsKey(path) && ALL.get(path).containsKey(method.toUpperCase());
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
