package io.qross.app;

import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.parser.ParserEmulationProfile;
import com.vladsch.flexmark.util.ast.Node;
import com.vladsch.flexmark.util.data.MutableDataSet;

import java.util.Collections;

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

    public Marker colorCodes(boolean lineNumbers) {
        content = content.replace("<pre><code", "<textarea coder=\"yes\" read-only=\"true\"" + (lineNumbers ? "" : " line-numbers=\"false\""))
                .replaceAll("\\s+</code></pre>", "</textarea>")
                .replace("class=\"language-sql\"", "mode=\"text/x-pql\"")
                .replace("class=\"language-xml\"", "mode=\"text/xml\"")
                .replace("class=\"language-", "mode=\"text/x-");

        return this;
    }

    public Marker transform() {
        content = Marker.markdownToHtml(content);
        format = Marker.HTML;
        //external links
        content = content.replace(" href=\"http", " target=\"_blank\" href=\"http");

        return this;
    }

    public String getContent() {
        return content;
    }
}
