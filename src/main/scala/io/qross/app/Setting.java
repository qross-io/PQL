package io.qross.app;

import io.qross.ext.TypeExt;
import io.qross.setting.Global;
import io.qross.setting.Properties;

public class Setting {

    public static String OneApiManagementKey = Properties.get("oneapi.management.key", "123456");
    public static String OneApiServiceName = Properties.get("oneapi.service.name", "");
    public static String OneApiSecurityMode = Properties.get("oneapi.security.mode", "none");
    public static int OneApiSecretKeyTTL = Integer.parseInt(Properties.get("oneapi.secret.key.ttl", "3600"));
    public static int OneApiSecretKeyDigit = Integer.parseInt(Properties.get("oneapi.secret.key.digit", "16"));
    public static String OneApiResourceDirs = Properties.get("oneapi.resources.dirs", "");
    public static String OneApiTokenList = Properties.get("oneapi.token.list", "");
    public static String OneApiAccessOpen = Properties.get("oneapi.access.open", "");
    public static String OneApiAccessPermit = Properties.get("oneapi.access.permit", "");

    public static String VoyagerDirectory = Properties.get("voyager.directory", "/templates/");
    public static String VoyagerConnection = Properties.get("voyager.connection", "jdbc.default");
    public static String VoyagerCharset = Properties.get("voyager.charset", Global.CHARSET());
    public static String VoyagerLanguage = Properties.get("voyager.language", Global.VOYAGER_LANGUAGE());
    public static String VoyagerStaticSite = Properties.get("voyager.static.site", "");
    public static String VoyagerGallerySite = Properties.get("voyager.gallery.site", "");
    public static boolean VoyagerCacheEnabled = TypeExt.StringExt(Properties.get("voyager.cache.enabled", "false")).toBoolean(false);

    public static void handleArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch(args[i].toLowerCase()) {
                case "--properties":
                    if (i + 1 < args.length) {
                        Properties.loadLocalFile(args[i+1], true);
                    }
                    break;
                case "--oneapi.service.name":
                    if (i + 1 < args.length) {
                        OneApiServiceName = args[i+1];
                    }
                    break;
                case "--oneapi.resources.dirs":
                    if (i + 1 < args.length) {
                        OneApiResourceDirs = args[i=1];
                    }
                    break;
                case "--voyager.directory":
                    if (i + 1 < args.length) {
                        VoyagerDirectory = args[i+1];
                    }
                    break;
                case "--voyager.connection":
                    if (i + 1 < args.length) {
                        VoyagerConnection = args[i+1];
                    }
                    break;
                case "--voyager.language":
                    if (i + 1 < args.length) {
                        VoyagerLanguage = args[i+1];
                    }
                    break;
                case "--voyager.charset":
                    if (i + 1 < args.length) {
                        VoyagerCharset = args[i+1];
                    }
                    break;
            }
        }
    }
}
