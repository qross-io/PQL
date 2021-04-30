package io.qross.app;

import io.qross.ext.TypeExt;

public class MarkerPath {

    public String path;
    public String title = "";

    public MarkerPath(String line) {
        if (line.contains(" \"")) {
            this.path = line.substring(0, line.indexOf(" \"")).trim();
            this.title = TypeExt.StringExt(line.substring(line.indexOf(" \"") + 1).trim()).removeQuotes();
        }
        else if (line.contains(" '")) {
            this.path = line.substring(0, line.indexOf(" '")).trim();
            this.title = TypeExt.StringExt(line.substring(line.indexOf(" '") + 1).trim()).removeQuotes();
        }
        else {
            this.path = line.trim();
        }
    }
}
