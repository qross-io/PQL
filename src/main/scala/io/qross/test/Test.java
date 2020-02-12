package io.qross.test;

import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.security.MD5;

public class Test {

    public static void main(String[] args) {

        DataAccess ds = new DataAccess("adb.dev");

        ds.executeExists("select 1 from tmp_aa");

        ds.close();
    }

}
