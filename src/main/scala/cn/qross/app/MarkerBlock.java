package cn.qross.app;

import cn.qross.fs.TextFile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MarkerBlock {

    private String content;
    private int type;
    private int indent = 0;
    private List<MarkerBlock> children = new ArrayList<>();
    private List<String> lines = new ArrayList<>();

    public static final int LINE = 0;
    public static final int CODE = 1;
    public static final int GAP = 2;
    public static final int TABLE = 3;
    public static final int UL = 4;
    public static final int OL = 5;
    public static final int H1 = 11;
    public static final int H2 = 12;
    public static final int H3 = 13;
    public static final int H4 = 14;
    public static final int H5 = 15;
    public static final int H6 = 16;
    public static final int HR = 17;

    public static final int PQL = 19;
    public static final int HTML_LINE = 20;
    public static final int HTML_NESTED_BLOCK = 21;
    public static final int HTML_NESTED_BLOCK_BEGIN = 22;
    public static final int HTML_NESTED_BLOCK_END = 23;
    public static final int HTML_BLOCK = 24;
    public static final int HTML_BLOCK_BEGIN = 25;
    public static final int HTML_BLOCK_END = 26;

    public static final Set<String> HTML_NESTED_BLOCKS = new HashSet<String>() {{
        add("DIV");
        add("TABLE");
        add("FOR");
        add("IF");
    }};
    public static final Set<String> HTML_BLOCKS = new HashSet<String>() {{
        add("TEMPLATE");
        add("MODEL");
        add("SCRIPT");
        add("STYLE");
        add("HEADER");
        add("FOOTER");
        add("SECTION");
        add("FORM");
        add("H1");
        add("H2");
        add("H3");
        add("H4");
        add("H5");
        add("H6");
    }};
    public static final Set<String> HTML_ALONE_LINES = new HashSet<String>() {{
        add("HR");
        add("LINK");
    }};

    public MarkerBlock(int type, String content) {
        this.type = type;
        this.content = content;
        this.lines.add(content);
    }

    public MarkerBlock(int type, String content, int indent) {
        this.type = type;
        this.content = content;
        this.lines.add(content);
        this.indent = indent;
        if (this.indent % 2 > 0) {
            this.indent -= 1;
        }
    }

    public void append(String content) {
        this.lines.add(content);
    }

    public void push(MarkerBlock line) {
        this.children.add(line);
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getContent() {
        if (this.lines.size() == 1) {
            return this.content;
        }
        else {
            return String.join(TextFile.TERMINATOR(), this.lines);
        }
    }
}
