package cn.qross.app

import scala.util.matching.Regex

object Cogo {

    private val scripts = Map[String, String](
        "popup" -> "root.popup.js",
        "message" -> "root.popup.js",
        "div-display" -> "root.div.js",
        "tab" -> "root.tab.js",
        "backtop" -> "root.backtop.js",
        "select" -> "root.select.js,select.css",
        "type-select" -> "root.select.js,select.css",
        "button" ->  "root.popup.js,root.button.js",
        "calendar" -> "root.calendar.js,calendar.css",
        "input-calendar" -> "root.calendar.js,calendar.css", //iconfont.css
        "clock" -> "root.clock.js,clock.css",
        "type-datetime" -> "root.datetimepicker.js,root.calendar.js,calendar.css,root.clock.js,clock.css,root.popup.js", //iconfont.css
        "treeview" -> "root.treeview.js,treeview.css",
        "editable" -> "root.editor.js",
        "table-data" -> "root.table.js,table.css",
        "coder" -> "root.coder.js,coder/codemirror.js,coder/codemirror.css,coder.css",
        "a-help" -> "root.help.js,root.popup.js", //iconfont.css
        "a-onclick" -> "root.anchor.js,root.popup.js",
        "chart" -> "charts/echarts.min.js,root.chart.js",
        "log" -> "root.log.js,log.css"
    )

    private val codes = Map[String, String](
        "pql" -> "sql.js",
        "sh" -> "shell.js",
        "shell" -> "shell.js",
        "java" -> "clike.js",
        "scala" -> "clike.js",
        "python" -> "python.js",
        "xml" -> "xml.js",
        "html" -> "htmlmixed.js",
        "css" -> "css.js",
        "sql" -> "sql.js",
        "csharp" -> "clike.js",
        "javascript" -> "javascript.js",
        "json" -> "javascript.js",
        "properties"-> "properties.js",
        "markdown" -> "markdown.js"
    )

    val template: String =
        """
            |<!DOCTYPE html>
            |<html lang="en">
            |   <head>
            |       <meta charset="UTF-8">
            |       <title>#{title}</title>
            |       <script type="text/javascript" src="@/root.js"></script>
            |       <script type="text/javascript" src="@/root.model.js"></script>
            |       <script type="text/javascript" src="@/root.anchor.js"></script>
            |       <script type="text/javascript" src="@/root.datetime.js"></script>
            |       <script type="text/javascript" src="@/root.animation.js"></script>
            |       <script type="text/javascript" src="@/root.input.js"></script>
            |       <link href="@/css/root/main.css" rel="stylesheet" type="text/css" />
            |       <link href="/iconfont.css" rel="stylesheet" type="text/css" />
            |       #{scripts}
            |   </head>
            |   <body>
            |       <div class="marker-frame">
            |           #{content}
            |       </div>
            |   </body>
            |</html>""".stripMargin

    def getScripts(content: String): String = {
        """(?i)<div[^>]+display=|\stab\b|<backtop\b|<select\b|\sselect=|\btype="select"|<button\b|<calendar\b|<clock\b|<input[^>]+type="calendar"|<treeview\b|\seditable\b|<table[^>]+data\b|coder=|<a\b[^>]+help=|\spopup\b|\smessage\b|<chart\b|<log\b""".r
            .findAllIn(content)
            .map(v => {
                val ms = "(?i)[a-z]+".r.findAllIn(v).map(_.toLowerCase()).toList
                if (ms.size >= 2) {
                    ms.head + "-" + ms.last
                }
                else {
                    ms.head
                }
            })
            .toSet
            .map(scripts(_))
            .flatMap(ref => {
                ref.split(",")
                    .map(r => {
                        if (r.endsWith(".js")) {
                            s"""<script type="text/javascript" src="@/$r"></script>"""
                        }
                        else if (r.contains("/")) {
                            s"""<link href="@/$r" rel="stylesheet" type="text/css" />"""
                        }
                        else {
                            s"""<link href="@/css/root/$r" rel="stylesheet" type="text/css" />"""
                        }
                    })
            })
            .toSeq
            .distinct
            .mkString("\n") + "\n" +
                List[Regex]("""(?i)coder=["']?([a-z]+)""".r,
                    """(?i)alternative=["']?([a-z, ]+)""".r)
                .flatMap(_.findAllMatchIn(content))
                .flatMap(_.group(1).toLowerCase().split(","))
                .map(_.trim())
                .distinct
                .filter(_ != "")
                .map(c => codes.getOrElse(c, c))
                .map(js => s"""<script type="text/javascript" src="@/coder/$js"></script>""")
                .mkString("\n")
    }
}