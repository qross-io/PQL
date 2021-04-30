package io.qross.pql.test;

import io.qross.app.Marker;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static int INTEGER = 111;
    public static String STRING = "HELLO";

    public static void hello(int i) {
        Console.writeLine(i);
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Object x = "1";

        if (x instanceof String) {
            Console.writeLine("HELLO WORLD");
        }


        System.exit(0);

        String result = Marker.openFile("/markdown/markdown.md").transform().colorCodes().getContent();

        Console.writeLine(result);

        System.exit(0);

        String connected = DataAccess.testConnection("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/qross?user=root&password=diablo&useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true");
        Console.writeLine(connected);

        System.exit(0);

        Console.writeLine(Test.class.getDeclaredField("INTEGER").get(null));
        Console.writeLine(Test.class.getDeclaredField("STRING").get(null));
    }
}