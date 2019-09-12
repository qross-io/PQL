package io.qross.app;

import io.qross.core.DataHub;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.pql.PQL;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class OneApi {

    public String name = "";
    public String method = "";
    public String defaultValue = "";
    public String sentences = "";

    // group -> name -> METHOD -> OneApi
    public static Map<String, Map<String, Map<String, OneApi>>> ALL = new HashMap<>();

    //read all api settings from resources:/api/
    public static void readAll() {
        ALL.clear();

        String[] files = ResourceDir.open("/api/").listFiles("*.sql");
        for (String file : files) {
            String path = file.substring(5, file.lastIndexOf("."));
            if (!ALL.containsKey(path)) {
                ALL.put(path, new LinkedHashMap<>());
            }

            String[] APIs = ResourceFile.open(file).content().split("##");

            for (int i = 0; i < APIs.length; i++) {
                APIs[i] = APIs[i].trim();
                if (!APIs[i].isEmpty()) {
                    String[] API = APIs[i].split("\\|", -1);
                    if (API.length >= 3) {
                        OneApi api = new OneApi();
                        api.name = API[0].trim();
                        api.method = API[1].trim().toUpperCase();
                        if (API.length == 3) {
                            api.sentences = API[2].trim();
                        }
                        else if (API.length == 4) {
                            api.defaultValue = API[2].trim();
                            api.sentences = API[3].trim();
                        }

                        if (!ALL.get(path).containsKey(api.name)) {
                            ALL.get(path).put(api.name, new HashMap<>());
                        }
                        ALL.get(path).get(api.name).put(api.method, api);
                    }
                }
            }
        }
    }

    public static Object request(String group, String name, String method, Map<String, String[]> args) {
        return request(group, name, method, args, DataHub.DEFAULT());
    }

    public static Object request(String group, String name, String method, Map<String, String[]> args, DataHub dh) {

        if (!OneApi.contains(group, name, method)) {
            OneApi.readAll();
        }

        if (OneApi.contains(group, name, method)) {
            OneApi api = OneApi.pick(group, name, method);

            if (api.method.equalsIgnoreCase(method)) {

                return new PQL(api.sentences, dh)
                        .place(args)
                        .place(api.defaultValue)
                        .run();
            }
            else {
                return "{\"error\": \"WRONG METHOD.\"}";
            }
        }

        return "{\"error\": \"WRONG or MISS path/method '" + group + "/" + name + " " + method + "'\"}";
    }

    public static boolean contains(String group, String name) {
        return ALL.containsKey(group) && ALL.get(group).containsKey(name);
    }

    public static boolean contains(String group, String name, String method) {
        return ALL.containsKey(group) && ALL.get(group).containsKey(name) && ALL.get(group).get(name).containsKey(method.toUpperCase());
    }

    public static OneApi pick(String group, String name, String method) {
        return ALL.get(group).get(name).get(method.toUpperCase());
    }
}
