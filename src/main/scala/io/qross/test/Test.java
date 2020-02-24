package io.qross.test;

import io.qross.ext.Console;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.jdbc.DataAccess;
import io.qross.security.MD5;
import io.qross.time.DateTime;

public class Test {

    public static void main(String[] args) {
        String[] files = ResourceDir.open("/sql/").listFiles("*.sql");
        for (String file : files) {
            String content = ResourceFile.open(file).content();
            Console.writeLine("");
            Console.writeLine(file);
            Console.writeLine("--------------------------------------------------");
            if (content.length() > 100) {
                Console.writeLine(content.substring(0, 100));
            }
            else {
                Console.writeLine(content);
            }
            Console.writeLine("--------------------------------------------------");
            Console.writeLine("");
        }
    }
}