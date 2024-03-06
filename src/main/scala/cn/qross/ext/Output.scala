package cn.qross.ext

import cn.qross.core.{DataHub, DataTable}
import cn.qross.jdbc.DataSource
import cn.qross.setting.Global
import cn.qross.time.DateTime

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

trait Output {

    protected var DEBUG: Boolean = Global.DEBUG
    protected var LOG_FORMAT = "text"

    //是否启用调试模式
    def debugging: Boolean = DEBUG

    def log(format: String = "text"): Unit = {
        LOG_FORMAT = format
    }

    def logFormat: String = LOG_FORMAT

    // ---------- Output ----------

    def writeLine(message: Any): Unit = {
        if (LOG_FORMAT == "html") {
            println(s"$message<br/>")
        }
        else {
            println(message)
        }
    }

    def writeHeader(title: Any, header: Int = 3): Unit = {
        if (LOG_FORMAT == "html") {
            println(s"<h$header>$title</h$header>")
        }
        else {
            println(title)
        }
    }

    def writeParagraph(messages: Any*): Unit = {
        if (LOG_FORMAT == "html") {
            print("<p>")
        }
        for (i <- messages.indices) {
            print(messages(i))
            if (LOG_FORMAT == "html" && i < messages.length - 1) {
                print("<br/>")
            }
            println()
        }
        if (LOG_FORMAT == "html") {
            print("</p>")
        }
        println()
    }

    def writeCode(code: String, language: String = "pql"): Unit = {
        if (LOG_FORMAT == "html") {
            println(s"""<textarea mode="$language">$code</textarea>""")
        }
        else {
            println()
            println(code)
        }
    }

    def writeMessage(message: Any): Unit = {
        writeSealLine("info", message)
    }

    def writeDebugging(message: Any): Unit = {
        writeSealLine("debug", message)
    }

    def writeWarning(message: Any): Unit = {
        writeSealLine("warn", message)
    }

    def writeException(message: Any): Unit = {
        writeSealLine("error", message)
        if (LOG_FORMAT == "html") {
            print(s"""<span class="datetime">${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")}</span> <span class="seal-error">[ERROR]</span> <span class="error">$message</span><br/>""")
        }
        else {
            System.err.println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [ERROR] " + message)
        }
    }

    def writeSealLine(seal: String, message: Any): Unit = {
        if (LOG_FORMAT == "html") {
            println(s"""<span class="datetime">${DateTime.now.getString("yyyy-MM-dd HH:mm:ss")}</span> <span class="seal-$seal">[${seal.toUpperCase()}]</span> <span class="$seal">$message</span><br/>""")
        }
        else {
            println(DateTime.now.getString("yyyy-MM-dd HH:mm:ss") + " [ERROR] " + message)
        }
    }

    def writeTable(table: DataTable, limit: Int = 20): Unit = {
        if (LOG_FORMAT == "html") {
            println(table.toHtmlString(20))
            println(s"""<p class="rows"><span class="rows-number">${table.size}</span> rows.</p>""")
        }
        else {
            table.show(limit)
        }
    }

    def writeAffected(rows: Int): Unit = {
        if (LOG_FORMAT == "html") {
            println(s"""<p class="rows"><span class="rows-number">$rows</span> row(s) affected.</p>""")
        }
        else {
            println(s"$rows row(s) affected. ")
        }
    }
}
