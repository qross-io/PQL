package io.qross.test;

import io.qross.app.OneApi;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.net.Redis;

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static int INTEGER = 10;
    public static String STRING = "HELLO";

    public static void hello() {

    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Matcher m = Pattern.compile("(a)b", Pattern.CASE_INSENSITIVE).matcher("abcefg");
        if (m.find()) {
            Console.writeLine(m.groupCount());
        }

        Console.writeLine(Test.class.getDeclaredField("INTEGER").get(null));
        Console.writeLine(Test.class.getDeclaredField("STRING").get(null));
    }
}