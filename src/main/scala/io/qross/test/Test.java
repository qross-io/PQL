package io.qross.test;

import io.qross.app.OneApi;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.net.Redis;

public class Test {
    public static void main(String[] args) {
        String x = "/BOOT-INF/classes/api/note.sql";
        String y = "/api/";

        Console.writeLine(x.substring(x.indexOf(y)));
    }
}