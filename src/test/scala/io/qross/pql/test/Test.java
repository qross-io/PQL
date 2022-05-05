package io.qross.pql.test;

import io.qross.app.Marker;
import io.qross.ext.Console;
import io.qross.ext.TypeExt;
import io.qross.jdbc.DataAccess;
import io.qross.net.Json;
import io.qross.setting.Environment;
import io.qross.setting.Properties;

import java.util.Arrays;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static int INTEGER = 111;
    public static String STRING = "HELLO";

    public static void hello(String content) {

        Console.writeLine(Pattern.CASE_INSENSITIVE);
        Console.writeLine(Pattern.MULTILINE);
        Console.writeLine(Pattern.DOTALL);
        Console.writeLine(Pattern.UNICODE_CASE);


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

    public static void main(String[] args) {

        Arrays.stream("1,2,,".split(",", -1)).forEach(System.out::println);



        System.exit(0);

        Pattern p = Pattern.compile("(?<month>\\d{2})/(?<day>\\d{2})/(?<year>\\d{4})");
        Matcher m = p.matcher("Today is 04/24/2022.");
        while(m.find()) {
            Console.writeLine(m.group("year") + "-" + m.group("month") + "-" + m.group("day"));
        }
    }


    public static void main1(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Console.writeLine();

        Map<String, String> json = new java.util.HashMap<String, String>();
        json.put("a", "b");

        System.out.println(Json.serialize(json));

        System.exit(0);

        Pattern p = Pattern.compile("(?<!/)/([#a-z0-9%,.\\s]+):([\\s\\S]*?[^<])/(?![a-z]+>)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher("hello/gray:因为参数的不同，才能让模板生成不同的逻辑。在配置工作流时需要配置这些参数，请一定配置完整。当模板逻辑改变时会<a onclick+=\"/api/system/command-template-parameters-reload?template_id=&(id)\" onclick+success-=\"reload: #Parameters\" confirm-text=\"确定要重新识别逻辑中的所有参数吗？\">自动获取逻辑中参数</a>，可以在此基础上再进行编辑。双击参数的某一项开始编辑，编辑完按回车自动保存。在最后一行空白行可以添加新的参数，输入完成后点击“添加”按钮完成添加。/world");
        if (m.find()) {
            Console.writeLine(m.group(0));
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