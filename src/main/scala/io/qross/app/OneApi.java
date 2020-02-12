package io.qross.app;

import io.qross.core.DataHub;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.fs.Directory;
import io.qross.fs.FileReader;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.jdbc.DataAccess;
import io.qross.pql.PQL;
import io.qross.setting.Properties;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.util.*;

public class OneApi {

    public String path = "";
    public String method = "";
    public String defaultValue = "";
    public String sentences = "";
    public Set<String> allowed = new HashSet<>();

    // path -> METHOD -> OneApi
    public static Map<String, Map<String, OneApi>> ALL = new HashMap<>();
    // token -> allowed name
    public static Map<String, String> TOKENS = new HashMap<>();

    //read all api settings from resources:/api/
    public static void readAll() {
        ALL.clear();

        //从jar包resources目录加载接口数据
        if (!Setting.OneApiResourceDirs.isEmpty()) {
            String[] dirs = Setting.OneApiResourceDirs.split(";");
            for (String dir : dirs) {
                String[] files = ResourceDir.open(dir).listFiles("*.sql");
                for (String file: files) {
                    readAPIs(file.substring(dir.length(), file.lastIndexOf(".")), ResourceFile.open(file).content().split("##"));
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
        if (!Setting.OneApiServiceName.isEmpty()) {
            DataAccess ds = new DataAccess(Setting.OneApiServiceConnection);
            DataTable APIs = ds.executeDataTable("SELECT * FROM qross_api_in_one WHERE service_name=?", Setting.OneApiServiceName);
            for (DataRow API : APIs.getRowList()) {
                OneApi api = new OneApi();
                api.path = API.getString("path");
                api.method = API.getString("method").toUpperCase();
                api.sentences = API.getString("pql");
                api.defaultValue = API.getString("default_value");

                if (!ALL.containsKey(api.path)) {
                    ALL.put(api.path, new LinkedHashMap<>());
                }
                ALL.get(api.path).put(api.method, api);
            }
            APIs.clear();

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
                if (API.length >= 3) {
                    OneApi api = new OneApi();
                    if (api.path.endsWith("/")) {
                        api.path = path + API[0].trim();
                    }
                    else {
                        api.path = path + "/" + API[0].trim();
                    }
                    api.method = API[1].trim().toUpperCase();
                    if (API.length == 3) {
                        api.sentences = API[2].trim();
                    }
                    else if (API.length == 4) {
                        if (API[2].contains("=")) {
                            api.defaultValue = API[2].trim();
                        }
                        else {
                            api.allowed.addAll(Arrays.asList(API[2].trim().split(",")));
                        }
                        api.sentences = API[3].trim();
                    }
                    else if (API.length == 5) {
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
                    ALL.get(api.path).put(api.method, api);
                }
            }
        }
    }

    //refresh all api
    public static int refresh() {
        OneApi.readAll();
        return OneApi.ALL.size();
    }

    public static Object request(String path) {
        return request(path, DataHub.DEFAULT());
    }

    public static Object request(String path, DataHub dh) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();

            String method = request.getMethod();
            if (!OneApi.contains(path, method)) {
                OneApi.readAll();
            }

            if (OneApi.contains(path, method)) {
                OneApi api = OneApi.pick(path, method);

                String security = Properties.get("oneapi.security.enabled");

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
                    return new PQL(api.sentences, dh)
                            .place(request.getParameterMap())
                            .place(api.defaultValue)
                            .run();
                } else {
                    return "{\"error\": \"Access denied\"}";
                }
            } else {
                return "{\"error\": \"WRONG or MISS path/Method '" + path + " " + method + "'\"}";
            }
        }
        else {
            return "{\"error\": \"Need spring boot environment.\"}";
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
