package io.qross.app;

import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.parser.ParserEmulationProfile;
import com.vladsch.flexmark.util.ast.Node;
import com.vladsch.flexmark.util.data.MutableDataSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Marker {

    public static final int MARKDOWN = 1;
    public static final int HTML = 2;

    private String content;
    private int format = Marker.MARKDOWN;

    public Marker(String content) {
        this.content = content;
    }

    public static String markdownToHtml(String content) {
        MutableDataSet options = new MutableDataSet();
        options.setFrom(ParserEmulationProfile.MARKDOWN);

        // enable table parse!
        options.set(Parser.EXTENSIONS, Collections.singletonList(TablesExtension.create()));
        //Arrays.asList(TablesExtension.create())

        Parser parser = Parser.builder(options).build();
        HtmlRenderer renderer = HtmlRenderer.builder(options).build();

        Node document = parser.parse(content);

        return renderer.render(document);
    }

    public static Marker openFile(String path) {
        return new Marker(io.qross.fs.SourceFile.read(path));
    }

    public static Marker open(String content) {
        return new Marker(content);
    }

    public Marker removeHeader() {
        if (format == Marker.MARKDOWN) {
            if (content.startsWith("# ")) {
                content = content.replaceAll("^# [\\s\\S]+?\\n", "");
            }
        }
        else {
            content = content.replaceFirst("<h1>[\\s\\S]+?</h1>", "");
        }
        return this;
    }

    public Marker replaceInMarkdown(String old, String replacement) {
        if (format == Marker.MARKDOWN) {
            content = content.replace(old, replacement);
        }

        return this;
    }

    public Marker replaceAllInMarkdown(String regex, String replacement) {
        if (format == Marker.MARKDOWN) {
            content = content.replaceAll(regex, replacement);
        }

        return this;
    }

    public Marker replaceInHtml(String old, String replacement) {
        if (format == Marker.MARKDOWN) {
            transform();
        }
        content = content.replace(old, replacement);

        return this;
    }

    public Marker replaceAllInHtml(String regex, String replacement) {
        if (format == Marker.MARKDOWN) {
            transform();
        }
        content = content.replaceAll(regex, replacement);

        return this;
    }

    public Marker colorCodes() {
        return colorCodes(true);
    }

    public Marker colorCodes(boolean lineNumbers) {
        content = content.replace("<pre><code", "<textarea coder=\"yes\" read-only=\"true\"" + (lineNumbers ? "" : " line-numbers=\"false\""))
                .replaceAll("\\s+</code></pre>", "</textarea>")
                .replace("class=\"language-", "mode=\"");

        return this;
    }

    public Marker referSite() {
        //static site  @ or %
        Pattern p = Pattern.compile("\\s(src|href)=\"([@|%])", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), " " + m.group(1) + "=\"" + (m.group(2).equals("@") ? Setting.VoyagerStaticSite : Setting.VoyagerGallerySite));
        }

        return this;
    }

    public Marker transform() {

        List<String> pql = new ArrayList<>();

        // embedded PQL
        Pattern p = Pattern.compile("<%[\\s\\S]+?%>", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~pql[" + pql.size() + "]");
            pql.add(m.group(0));
        }

        // /green:我是绿色/
        // /16:abc/
        // //b,i,u,s
        // /green,b:绿色粗体/
        // primary, darker, lighter

        p = Pattern.compile("/([#a-z0-9,\\s]+):([^/]+)/", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            StringBuilder sb = new StringBuilder();
            for (String value : m.group(1).toLowerCase().split(",")) {
                String s = value.trim();
                if (s.matches("^\\d+$")) {
                    s = "font-size: " + (Float.parseFloat(s) / 16f) + "em";
                }
                else if (s.length() == 1) {
                    switch (s) {
                        case "b":
                            s = "font-weight: bold";
                            break;
                        case "i":
                            s = "font-style: italic";
                            break;
                        case "u":
                            s = "text-decoration: underline";
                            break;
                        case "s":
                            s = "text-decoration: line-through";
                            break;
                    }
                }
                else {
                    if (s.matches("^(primary|darker|lighter)$")) {
                        s = "var(--" + s + ")";
                    }
                    s = "color: " + s;
                }
                sb.append(s).append("; ");
            }

            String text = m.group(2);
            Pattern q = Pattern.compile(":\\s*([#a-z0-9]+)\\s*$", Pattern.CASE_INSENSITIVE);
            Matcher n = q.matcher(text);
            if (n.find()) {
                sb.append("background-color: ").append(n.group(1)).append("; ")
                    .append("border-radius: 0.2rem; ")
                    .append("padding-inline-start: 0.2rem; ")
                    .append("padding-inline-end: 0.2rem;");

                text = text.substring(0, text.lastIndexOf(":"));
            }

            content = content.replace(m.group(0), "<span style=\"" + sb.toString() + "\">" + text + "</span>");
        }

        // -- n --
        p = Pattern.compile("--\\s*(\\d+)\\s*--");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "<div style=\"height: " + m.group(1) + "px\"></div>");
        }

        // -| n |-
        p = Pattern.compile("-\\|\\s*(\\d+)\\s*\\|-");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "<span style=\"display: inline-block; width: " + m.group(1) + "px\"></span>");
        }

        content = Marker.markdownToHtml(content);
        format = Marker.HTML;

        //restore PQL
        p = Pattern.compile("~pql\\[(\\d+)\\]");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), pql.get(Integer.parseInt(m.group(1))));
        }

        //external links
        content = content.replace(" href=\"http", " target=\"_blank\" href=\"http");

        return this;
    }

    public String getTitle() {
        String title = "";
        if (content.contains("<h1>")) {
            title = content.substring(content.indexOf("<h1>") + 4, content.indexOf("</h1>"));
        }

        return title;
    }

    public String getContent() {
        return content;
    }

    public String getCogoContent() {
        return Cogo.template().replace("#{title}", this.getTitle()).replace("#{scripts}", Cogo.getScripts(content)).replace("#{content}", content);
    }
}
