package io.qross.ext

import io.qross.time.DateTime

object Output {

    def writeLine(messages: Any*): Unit = {
        for (message <- messages) {
            print(message)
        }
        println()
    }

    def writeLine(message: String): Unit = {
        println(message)
    }

    def writeDotLine(delimiter: String, messages: Any*): Unit = {
        for (i <- 0 until messages.length) {
            if (i > 0) print(delimiter)
            print(messages(i))
        }
        println()
    }

    def writeLines(messages: Any*): Unit = {
        for (message <- messages) {
            println(message)
        }
    }

    def writeMessage(messages: Any*): Unit = {
        for (message <- messages) {
            println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [INFO] " + message)
        }
    }

    def writeMessage(message: String): Unit = {
        println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [INFO] " + message)
    }

    def writeWarning(messages: Any*): Unit = {
        for (message <- messages) {
            println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [WARN] " + message)
        }
    }

    def writeDebugging(messages: Any*): Unit = {
        for (message <- messages) {
            println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [DEBUG] " + message)
        }
    }

    def writeLineWithSeal(seal: String, message: String): Unit = {
        println(s"${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")} [$seal] $message")
    }

    def writeException(messages: Any*): Unit = {
        for (message <- messages) {
            System.err.println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [ERROR] " + message)
        }
    }
}
