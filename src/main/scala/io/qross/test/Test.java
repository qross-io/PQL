package io.qross.test;

import io.qross.app.OneApi;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.jdbc.DataSource;
import io.qross.net.Redis;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static int INTEGER = 10;
    public static String STRING = "HELLO";

    public static void hello() {

    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        String connected = DataAccess.testConnection("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/qross?user=root&password=diablo&useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true");
        Console.writeLine(connected);

        System.exit(0);

        Matcher m = Pattern.compile("(?<!/)/([#a-z0-9,\\s]+):([^/]+)/", Pattern.CASE_INSENSITIVE).matcher("http://localhost:8080/abc");
        if (m.find()) {
            Console.writeLine(m.group(0));
        }

        Console.writeLine("-----------------");

        Console.writeLine(Test.class.getDeclaredField("INTEGER").get(null));
        Console.writeLine(Test.class.getDeclaredField("STRING").get(null));
    }
}