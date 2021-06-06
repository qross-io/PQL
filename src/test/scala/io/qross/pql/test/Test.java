package io.qross.pql.test;

import io.qross.app.Marker;
import io.qross.app.OneApi;
import io.qross.app.Setting;
import io.qross.ext.Console;
import io.qross.ext.TypeExt;
import io.qross.fs.ResourceDir;
import io.qross.fs.ResourceFile;
import io.qross.jdbc.DataAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.qross.app.OneApi.readAPIs;

public class Test {

    public static int INTEGER = 111;
    public static String STRING = "HELLO";

    public static void hello(String content) {

        Pattern p = Pattern.compile("@(\\{[\\s\\S]+?})");//(?=\s*\n)
        Matcher m = p.matcher(content);
        while (m.find()) {
            String match = m.group(1);
            if (match.contains(":")) {
                int stack = TypeExt.StringExt(match).stackAllPairOf("\\{", "\\}", 0);
                if (stack > 0) {
                    int index = content.indexOf(match);
                    int pair = TypeExt.StringExt(content).indexPairOf("{", "}", index + match.length(), stack);
                    if (pair > -1) {
                        match += content.substring(index, pair + 1);
                    }
                }
            }
            Console.writeLine(match);
        }
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        readAPIs("/api", ResourceFile.open("/api/report.sql").content());

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