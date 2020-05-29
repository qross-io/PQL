package io.qross.test;

import io.qross.app.OneApi;
import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;

public class Test {

    public static void main(String[] args) {
        Console.writeLine(DataAccess.QROSS().executeDataTable("SELECT id, status FROM qross_tasks LIMIT 10"));
    }

    public void call(String message) {
        Console.writeLine(message);
    }
}