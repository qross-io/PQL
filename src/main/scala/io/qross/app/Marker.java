package io.qross.app;

import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.parser.ParserEmulationProfile;
import com.vladsch.flexmark.util.ast.Node;
import com.vladsch.flexmark.util.data.MutableDataSet;
import io.qross.ext.Console;
import io.qross.ext.TypeExt;
import io.qross.fs.TextFile;

import java.util.*;
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

    // unfinished
    public static String MarkdownToHTML(String content) {

        Stack<String> closing = new Stack<>(); //to be closed
        int stack = 0;

        Map<String, MarkerPath> refers = new HashMap<>();
        List<String> codes = new ArrayList<>();
        List<MarkerBlock> blocks = new ArrayList<>();

        //embedded PQL
        Pattern p = Pattern.compile("<%[\\s\\S]+?%>", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~pql[" + codes.size() + "]");
            codes.add(m.group(0));
        }

        // code
        p = Pattern.compile("```([a-zA-Z]+\b)?([\\s\\S]+?)```");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~code[" + codes.size() + "]");
            if (m.group(1) != null) {
                codes.add("<textarea coder=\"" + m.group(1).toLowerCase() + "\" read-only=\"true\" line-numebrs=\"true\">" + m.group(2) + "</textarea>");
            }
            else {
                codes.add("<pre><code>" + m.group(2) + "</code></pre>");
            }
        }

        // /green:我是绿色/
        // /16:abc/
        // //b,i,u,s
        // /green,b:绿色粗体/
        // primary, darker, lighter
        // 200%
        // Consolas
        p = Pattern.compile("(?<!/)/([#a-z0-9%,\\s]+):([\\s\\S]*?[^<])/(?![a-z]+>)", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            StringBuilder sc = new StringBuilder();
            for (String value : m.group(1).split(",")) {
                String s = value.trim();
                if (s.matches("^\\d+$")) {
                    s = "font-size: " + (Float.parseFloat(s) / 16f) + "rem";
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
                else if (Pattern.compile("^[A-Z]").matcher(s).find()) {
                    s = "font-family: " + s;
                }
                else if (s.endsWith("%")) {
                    s = "line-height: " + s;
                }
                else {
                    if (s.matches("^(primary|darker|lighter)$")) {
                        s = "var(--" + s + ")";
                    }
                    s = "color: " + s;
                }
                sc.append(s).append("; ");
            }

            String text = m.group(2);
            Pattern q = Pattern.compile(":\\s*([#a-z0-9]+)\\s*$", Pattern.CASE_INSENSITIVE);
            Matcher n = q.matcher(text);
            if (n.find()) {
                sc.append("background-color: ")
                    .append(n.group(1))
                    .append("; ")
                    .append("border-radius: 0.2rem; ")
                    .append("padding-inline-start: 0.2rem; ")
                    .append("padding-inline-end: 0.2rem;");

                text = text.substring(0, text.lastIndexOf(":"));
            }

            content = content.replace(m.group(0), "<span style=\"" + sc.toString() + "\">" + text + "</span>");
        }

        // **bold** & *italic*
        p = Pattern.compile("(?<!\\*)\\*(?!\\*)(.*?)(?<!\\*)\\*|\\*(?!\\*)(.*?)(?<!\\*)\\*(?!\\*)|\\*{2}([^*].*?)\\*{2}");
        m = p.matcher(content);
        while (m.find()) {
            if (m.group(1) == null) {
                content = content.replace(m.group(0), "<b>" + m.group(2) + "</b>");
            }
            else {
                content = content.replace(m.group(0), "<i>" + m.group(1) + "</i>");
            }
        }
        while ((m = p.matcher(content)).find()) {
            if (m.group(1) == null) {
                content = content.replace(m.group(0), "<b>" + m.group(2) + "</b>");
            }
            else {
                content = content.replace(m.group(0), "<i>" + m.group(1) + "</i>");
            }
        }

        // `code`
        p = Pattern.compile("(?<!`)`(?!`)(.*?)(?<!`)`(?!`)");
        m = p.matcher(content);
        while (m.find()) {
            Console.writeLine(m.group(0));
            content = content.replace(m.group(0), "<code>" + m.group(1) + "</code>");
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

        String[] lines = content.split(TextFile.TERMINATOR(), -1);
        List<String> indexes = new ArrayList<>();

        for (String line : lines) {
            if (closing.isEmpty()) {
                p = Pattern.compile("^(######|#####|####|###|##|#|===|\\*\\*\\*|---|~code\\[|\\* |\\+ |- |\\d+\\. |```(?!`)|\\[.+?]:|\\|)", Pattern.CASE_INSENSITIVE);
                m = p.matcher(line);
                if (m.find()) {
                    switch (m.group(1)) {
                        case "# ":
                            blocks.add(new MarkerBlock(MarkerBlock.H1, line.substring(2)));
                            break;
                        case "## ":
                            blocks.add(new MarkerBlock(MarkerBlock.H2, line.substring(3)));
                            break;
                        case "### ":
                            blocks.add(new MarkerBlock(MarkerBlock.H3, line.substring(4)));
                            break;
                        case "===":
                            if (blocks.size() > 0) {
                                blocks.get(blocks.size() - 1).setType(MarkerBlock.H1);
                            }
                            break;
                        case "* ":
                        case "+ ":
                        case "- ":
                            blocks.add(new MarkerBlock(MarkerBlock.UL, line.substring(2)));
                            break;
                        case "***":
                        case "---":
                            blocks.add(new MarkerBlock(MarkerBlock.HR, ""));
                            break;
                        case "#### ":
                            blocks.add(new MarkerBlock(MarkerBlock.H4, line.substring(5)));
                            break;
                        case "##### ":
                            blocks.add(new MarkerBlock(MarkerBlock.H5, line.substring(6)));
                            break;
                        case "###### ":
                            blocks.add(new MarkerBlock(MarkerBlock.H6, line.substring(7)));
                            break;
                        case "~code[":
                            blocks.add(new MarkerBlock(MarkerBlock.CODE, codes.get(Integer.parseInt(line.substring(6, line.indexOf("]"))))));
                            break;
                        case "|":
                            blocks.add(new MarkerBlock(MarkerBlock.TABLE, line));
                            break;
                        default:
                            // [n]:
                            if (m.group(1).startsWith("[")) {
                                refers.put(m.group(1).substring(1, m.group(1).indexOf("]:")), new MarkerPath(line.substring(line.indexOf("]:") + 2)));
                            }
                            // n.
                            else {
                                blocks.add(new MarkerBlock(MarkerBlock.OL, line.substring(m.group(1).indexOf(". ") + 2)));
                            }
                            break;
                    }
                }
                else if (line.trim().equals("")) {
                    blocks.add(new MarkerBlock(MarkerBlock.GAP, ""));
                }
                else if (line.startsWith(" ") || line.startsWith("\t")) {
                    boolean found = false;
                    int indent = 0;
                    p = Pattern.compile("^\\s+");
                    m = p.matcher(line);
                    if (m.find()) {
                        indent = m.group(0).replace("\t", "    ").length();
                    }

                    p = Pattern.compile("^\\s+(\\* |\\+ |- |\\d+. |~code\\[|\\|)");
                    m = p.matcher(line);
                    if (m.find()) {
                        if (m.group(1).startsWith("*") || m.group(1).startsWith("+") || m.group(1).startsWith("-")) {
                            blocks.add(new MarkerBlock(MarkerBlock.UL, line.substring(line.indexOf(m.group()) + 2), indent));
                        }
                        else if (m.group(1).contains(". ")) {
                            blocks.add(new MarkerBlock(MarkerBlock.UL, line.substring(line.indexOf(". ") + 2), indent));
                        }
                        else if (m.group(1).equals("|")) {
                            blocks.add(new MarkerBlock(MarkerBlock.TABLE, line.substring(indent), indent));
                        }
                        else {
                            blocks.add(new MarkerBlock(MarkerBlock.OL, codes.get(Integer.parseInt(m.group(1).substring(6, line.indexOf("]"))))));
                        }
                        found = true;
                    }

                    // html tag
                    if (!found) {
                        p = Pattern.compile("^\\s+<([a-z]+|h[1-6])\\b", Pattern.CASE_INSENSITIVE);
                        m = p.matcher(line);
                        if (m.find()) {
                            String tag = m.group(1).toUpperCase();
                            if (MarkerBlock.HTML_NESTED_BLOCKS.contains(tag)) {
                                blocks.add(new MarkerBlock(MarkerBlock.HTML_NESTED_BLOCK, line));
                                stack = TypeExt.StringExt(line).stackAllPairOf("<" + tag, "</" + tag + ">", stack);
                                if (stack > 0) {
                                    closing.add(tag);
                                }
                            }
                            else if (MarkerBlock.HTML_BLOCKS.contains(tag)) {
                                blocks.add(new MarkerBlock(MarkerBlock.HTML_BLOCK, line));
                                if (!Pattern.compile("</" + tag + ">", Pattern.CASE_INSENSITIVE).matcher(line).find()) {
                                    closing.add(tag);
                                }
                            }
                            else if (MarkerBlock.HTML_ALONE_LINES.contains(tag)) {
                                blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                            }
                            else if (TypeExt.StringExt(line).isHTML()) {
                                blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                            }
                            else {
                                blocks.add(new MarkerBlock(MarkerBlock.LINE, line));
                            }
                        }
                        else {
                            p = Pattern.compile("^\\s*~pql[(\\d+)]\\s*$");
                            m = p.matcher(line);
                            if (m.find()) {
                                blocks.add(new MarkerBlock(MarkerBlock.PQL, codes.get(Integer.parseInt(m.group(1)))));
                            }
                            else if (TypeExt.StringExt(line).isHTML()) {
                                blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                            }
                            else {
                                blocks.add(new MarkerBlock(MarkerBlock.LINE, line.replaceAll("^\\s+", ""), indent));
                            }
                        }
                    }
                }
                else {
                    p = Pattern.compile("^<([a-z]+|h[1-6])\\b", Pattern.CASE_INSENSITIVE);
                    m = p.matcher(line);
                    if (m.find()) {
                        String tag = m.group(1).toUpperCase();
                        if (MarkerBlock.HTML_NESTED_BLOCKS.contains(tag)) {
                            //html nestable block
                            blocks.add(new MarkerBlock(MarkerBlock.HTML_NESTED_BLOCK, line));
                            stack = TypeExt.StringExt(line).stackAllPairOf("<" + tag, "</" + tag + ">", stack);
                            if (stack > 0) {
                                closing.add(tag);
                            }
                        }
                        else if (MarkerBlock.HTML_BLOCKS.contains(tag)) {
                            //html block
                            blocks.add(new MarkerBlock(MarkerBlock.HTML_BLOCK, line));
                            if (!Pattern.compile("</" + tag + ">", Pattern.CASE_INSENSITIVE).matcher(line).find()) {
                                closing.add(tag);
                            }
                        }
                        else if (MarkerBlock.HTML_ALONE_LINES.contains(tag)) {
                            //single html line, can be ignore
                            blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                        }
                        else if (TypeExt.StringExt(line).isHTML()) {
                            blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                        }
                        else {
                            // html internal
                            blocks.add(new MarkerBlock(MarkerBlock.LINE, line));
                        }
                    }
                    else {
                        p = Pattern.compile("^\\s*~pql[(\\d+)]\\s*$");
                        m = p.matcher(line);
                        if (m.find()) {
                            // pql line
                            blocks.add(new MarkerBlock(MarkerBlock.PQL, codes.get(Integer.parseInt(m.group(1)))));
                        }
                        else if (TypeExt.StringExt(line).isHTML()) {
                            blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                        }
                        else {
                            //normal line
                            blocks.add(new MarkerBlock(MarkerBlock.LINE, line.replaceAll("^\\s+", "")));
                        }
                    }
                }
            }
            else {
                blocks.add(new MarkerBlock(MarkerBlock.HTML_LINE, line));
                //find pair
                if (stack > 0) {
                    // html nestable tag
                    stack = TypeExt.StringExt(line).stackAllPairOf("<" + closing.lastElement() + "\\b", "</" + closing.lastElement() + ">", stack);
                    if (stack == 0) {
                        closing.pop();
                    }
                }
                else {
                    // html tag
                    if (Pattern.compile("</" + closing.lastElement() + ">", Pattern.CASE_INSENSITIVE).matcher(line).find()) {
                        closing.pop();
                    }
                }
            }
        }

        closing.clear();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < blocks.size(); i++) {
            MarkerBlock block = blocks.get(i);
            MarkerBlock previous = null;
            if (i > 0) {
                previous = blocks.get(i - 1);
            }
            MarkerBlock next = null;
            if (i < blocks.size() - 1) {
                next = blocks.get(i + 1);
            }
            switch (block.getType()) {
                case MarkerBlock.H1:
                    sb.append("<h1>").append(block.getContent()).append("</h1>");
                    break;
                case MarkerBlock.H2:
                    sb.append("<h2>").append(block.getContent()).append("</h2>");
                    break;
                case MarkerBlock.H3:
                    sb.append("<h3>").append(block.getContent()).append("</h3>");
                    break;
                case MarkerBlock.H4:
                    sb.append("<h4>").append(block.getContent()).append("</h4>");
                    break;
                case MarkerBlock.H5:
                    sb.append("<h5>").append(block.getContent()).append("</h5>");
                    break;
                case MarkerBlock.H6:
                    sb.append("<h6>").append(block.getContent()).append("</h6>");
                    break;
                case MarkerBlock.GAP:
                    if (previous != null) {
                        if (previous.getType() == MarkerBlock.LINE) {
                            sb.append("</p>");
                        }
                        else if (previous.getType() == MarkerBlock.UL) {
                            sb.append("</ul>");
                        }
                        else if (previous.getType() == MarkerBlock.OL) {
                            sb.append("</ol>");
                        }
                    }
                    break;
                case MarkerBlock.LINE:
                    if (previous == null || previous.getType() == MarkerBlock.GAP || previous.getType() == MarkerBlock.TABLE || (previous.getType() > MarkerBlock.H1 && previous.getType() <= MarkerBlock.HR)) {
                        sb.append("<p>");
                    }
                    sb.append(block.getContent());
                    break;
                case MarkerBlock.UL:
                    if (previous == null || closing.isEmpty() || !closing.lastElement().equals("UL")) {
                        sb.append("<ul>");
                    }
                    sb.append("<li>").append(block.getContent()).append("</li>");
                    break;
                case MarkerBlock.OL:

                    break;
                case MarkerBlock.CODE:

                    break;
                case MarkerBlock.TABLE:

                    break;
                case MarkerBlock.HR:
                    sb.append("<hr/>");
                    break;
            }
        }

        return ""; //sb.toString();
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
        content = content.replace("<pre><code", "<textarea read-only=\"true\"" + (lineNumbers ? "" : " line-numbers=\"false\""))
                .replaceAll("\\s+</code></pre>", "</textarea>")
                .replace("class=\"language-", "coder=\"");

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

        List<String> stash = new ArrayList<>();
        List<String> box = new ArrayList<>();

        // box embedded PQL
        Pattern p = Pattern.compile("<%[\\s\\S]+?%>", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~box[" + box.size() + "]");
            box.add(m.group(0));
        }

        //TEXTAREA
        p = Pattern.compile("(?i)<textarea\\b[\\s\\S]+?</textarea>", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~stash[" + stash.size() + "]");
            stash.add(m.group(0));
        }

        //code
        p = Pattern.compile("```\\S*\\s*(\\S[\\s\\S]*?\\S)\\s*```", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(1), "~stash[" + stash.size() + "]");
            stash.add(m.group(1));
        }

        //include
        p = Pattern.compile("<#\\s*(include|page)\\s+[\\s\\S]+?/>", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), "~box[" + box.size() + "]");
            box.add(m.group(0));
        }

        // /green:我是绿色/
        // /16:abc/
        // //b,i,u,s
        // /green,b:绿色粗体/
        // primary, darker, lighter
        // 200%
        // Consolas
        p = Pattern.compile("(?<!/)/([#a-z0-9%,.\\s]+):([\\s\\S]*?[^<])/(?![a-z]+>)", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            StringBuilder sb = new StringBuilder(); //style=
            StringBuilder cb = new StringBuilder(); //class=
            for (String value : m.group(1).split(",")) {
                String s = value.trim();
                if (s.matches("^\\d+$")) {
                    s = "font-size: " + (Float.parseFloat(s) / 16f) + "rem";
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
                else if (Pattern.compile("^[A-Z]").matcher(s).find()) {
                    s = "font-family: " + s;
                }
                else if (s.endsWith("%")) {
                    s = "line-height: " + s;
                }
                else if (s.startsWith(".")) {
                    cb.append(s.substring(1)).append(" ");
                    s = "";
                }
                else {
                    if (s.matches("^(primary|darker|lighter)$")) {
                        s = "var(--" + s + ")";
                    }
                    s = "color: " + s;
                }
                if (!s.isEmpty()) {
                    sb.append(s).append("; ");
                }
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

            content = content.replace(m.group(0), "<span" + (cb.length() > 0 ? " class=\"" + cb.toString() + "\"" : "") + " style=\"" + sb.toString() + "\">" + text + "</span>");
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

        // DIV
        content = content.replaceAll("(?i)<div\\b", "<block").replaceAll("(?i)</div>", "</block>");

        content = content.replace("&lt;", "~lt;").replace("&gt;", "~gt;").replace("&quot;", "~quot;");

        content = Marker.markdownToHtml(content);
        //restore html
        content = content.replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&quot;", "\"")
                    .replace("~lt;", "&lt;")
                    .replace("~gt;", "&gt;")
                    .replace("~quot;", "&quot;");

        //处理 DIV 元素
        p = Pattern.compile("<p>.*?<block\\b", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), m.group(0).substring(3));
        }
        p = Pattern.compile("</block>.*?</p>", Pattern.CASE_INSENSITIVE);
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), m.group(0).substring(0, m.group(0).length() - 4));
        }

        content = content.replaceAll("(?i)<block\\b", "<div").replaceAll("(?i)</block>", "</div>");

        format = Marker.HTML;

        //pop stash
        p = Pattern.compile("~stash\\[(\\d+)\\]");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), stash.get(Integer.parseInt(m.group(1))));
        }
        //recurse
        while ((m = p.matcher(content)).find()) {
            content = content.replace(m.group(0), stash.get(Integer.parseInt(m.group(1))));
        }

        //pop box
        p = Pattern.compile("<p>~box\\[(\\d+)]</p>");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), box.get(Integer.parseInt(m.group(1))));
        }

        p = Pattern.compile("~box\\[(\\d+)]");
        m = p.matcher(content);
        while (m.find()) {
            content = content.replace(m.group(0), box.get(Integer.parseInt(m.group(1))));
        }

        //external links & <table>
        content = content.replace(" href=\"http", " target=\"_blank\" href=\"http").replace("<table>", "<table marker>");

        return this;
    }

    public String getTitle() {
        String title = "";
        if (content.contains("<h1>")) {
            title = content.substring(content.indexOf("<h1>") + 4, content.indexOf("</h1>"));
        }

        return title;
    }

    public String getSummary() {
        String summary = "";
        if (content.contains("<p>")) {
            summary = content.substring(content.indexOf("<p>") + 3, content.indexOf("</p>"));
            summary = summary.replaceAll("<[^>]+?>|\\n", "");
        }

        return summary;
    }

    public String getContent() {
        return content;
    }

    public String getCogoContent() {
        return Cogo.template().replace("#{title}", this.getTitle()).replace("#{scripts}", Cogo.getScripts(content)).replace("#{content}", content);
    }
}
