package io.qross.ext;

import io.qross.time.DateTime;

public class Console {

    public static void writeLine(Object...messages) {
        for (Object message : messages) {
            System.out.print(message);
        }
        System.out.println();
    }

    public static void writeDotLine(String delimiter, Object... messages) {

        for (int i = 0; i < messages.length; i++) {
            if (i > 0) {
                System.out.print(delimiter);
            }
            System.out.print(messages[i]);
        }
        System.out.println();
    }

    public static void writeLines(Object...messages) {
        for (Object message : messages) {
            System.out.println(message);
        }
    }

    public static void writeMessage(Object... messages) {
        for (Object message : messages) {
            System.out.println(DateTime.now().getString("yyyy-MM-dd HH:mm:ss") + " [INFO] " + message);
        }
    }

    public static void writeWarning(Object... messages) {
        for (Object message : messages) {
            System.out.println(DateTime.now().getString("yyyy-MM-dd HH:mm:ss") + " [WARN] " + message);
        }
    }

    public static void writeDebugging(Object... messages) {
        for (Object message : messages) {
            System.out.println(DateTime.now().getString("yyyy-MM-dd HH:mm:ss") + " [DEBUG] " + message);
        }
    }

    public static void writeLineWithSeal(String seal, Object... messages) {
        for (Object message : messages) {
            System.out.println(DateTime.now().getString("yyyy-MM-dd HH:mm:ss") + " [" + seal + "] " + message);
        }
    }

    public static void writeException(Object... messages) {
        for (Object message : messages) {
            System.err.println(DateTime.now().getString("yyyy-MM-dd HH:mm:ss") + " [ERROR] " + message);
        }
    }
}
