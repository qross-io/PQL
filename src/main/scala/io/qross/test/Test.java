package io.qross.test;

import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;

public class Test {

    public static void main(String[] args) {
        DataAccess dc = DataAccess.QROSS;
        Console.writeLine(dc.executeDataTable("SELECT id, title FROM qross_jobs").count());
        dc.close();
    }

}
