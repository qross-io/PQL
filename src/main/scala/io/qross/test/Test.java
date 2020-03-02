package io.qross.test;

import io.qross.ext.Console;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.jdbc.DataAccess;
import io.qross.security.MD5;
import io.qross.setting.Properties;
import io.qross.time.DateTime;

public class Test {

    public static void main(String[] args) {

        Console.writeLine(Properties.getAllPropertyNames());
        Properties.loadResourcesFile("/dbs-dev.properties");
        Console.writeLine(Properties.getAllPropertyNames());
    }
}