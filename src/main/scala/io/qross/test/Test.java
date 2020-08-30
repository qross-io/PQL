package io.qross.test;

import io.qross.app.OneApi;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.net.Redis;

import java.lang.reflect.Field;

public class Test {

    public static int INTEGER = 10;
    public static String STRING = "HELLO";

    public static void hello() {

    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        Console.writeLine(Test.class.getDeclaredField("INTEGER").get(null));
        Console.writeLine(Test.class.getDeclaredField("STRING").get(null));
    }
}