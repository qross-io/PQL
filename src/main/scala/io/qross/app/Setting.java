package io.qross.app;

import io.qross.setting.Global;
import io.qross.setting.Properties;

public class Setting {

    public static String OneApiResourceDirs = Properties.get("oneapi.internal.dirs");
    public static String OneApiExternalDirs = Properties.get("oneapi.external.dirs");
    public static String OneApiServiceConnection = Properties.get("oneapi.service.connection");
    public static String OneApiServiceName = Properties.get("oneapi.service.name");

    public static String VoyagerDirectory = Properties.get("voyager.directory", "/templates/");
    public static String VoyagerExtension = Properties.get("voyager.extension", "html");
    public static String VoyagerConnection = Properties.get("voyager.connection", "jdbc.default");
    public static String VoyagerCharset = Properties.get("voyager.charset", Global.CHARSET());

    public static void handleArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch(args[i].toLowerCase()) {
                case "--properties":
                    if (i + 1 < args.length) {
                        Properties.loadLocalFile(args[i+1]);
                    }
                    break;
                case "--oneapi.internal.dir":
                    if (i + 1 < args.length) {
                        OneApiResourceDirs = args[i=1];
                    }
                    break;
                case "--oneapi.external.dir":
                    if (i + 1 < args.length) {
                        OneApiExternalDirs = args[i=1];
                    }
                    break;
                case "--oneapi.service.connection":
                    if (i + 1 < args.length) {
                        OneApiServiceConnection = args[i+1];
                    }
                    break;
                case "--oneapi.service.name":
                    if (i + 1 < args.length) {
                        OneApiServiceName = args[i+1];
                    }
                    break;
                case "--voyager.directory":
                    if (i + 1 < args.length) {
                        VoyagerDirectory = args[i+1];
                    }
                    break;
                case "--voyager.extension":
                    if (i + 1 < args.length) {
                        VoyagerExtension = args[i+1];
                    }
                    break;
                case "--voyager.connection":
                    if (i + 1 < args.length) {
                        VoyagerConnection = args[i+1];
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
