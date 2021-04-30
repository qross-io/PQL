package io.qross.app;

import java.util.regex.Pattern;

public class MarkerLinkOrImage {

    public static final int LINK = 1;
    public static final int IMAGE = 2;

    public int type = LINK;

    public MarkerLinkOrImage(String content) {
        if (content.startsWith("!")) {
            this.type = IMAGE;
        }

        //links and images
//        p = Pattern.compile("!?\\[(\\S+)]\\((\\.+?)\\)(\\(\\.+?\\))?!?");
//        m = p.matcher(content);
//        while (m.find()) {
//            content = content.replace(m.group(0), new MarkerLinkOrImage(m.group(0)).toHtml());
//        }
    }

    public String toHtml() {

        return "";
    }
}
