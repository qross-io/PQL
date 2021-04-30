package io.qross.app

object Cogo {

    private val scripts = Map[String, String](
        "popup" -> "root.popup.js",
        "div-display" -> "root.layout.js",
        "callout" -> "root.callout.js",
        "focusview" -> "root.focusview.js",
        "cookie" -> "root.storage.js",
        "backtop" -> "root.backtop.js",
        "select" -> "root.select.js,select.css",
        "button" ->  "root.popup.js,root.dialog.js,root.button.js",
        "calendar" -> "root.calendar.js,calendar.css",
        "input-calendar" -> "root.calendar.js,calendar.css,iconfont.css",
        "clock" -> "root.clock.js,clock.css",
        "type-datetime" -> "root.datetimepicker.js,root.calendar.js,calendar.css,iconfont.css,root.clock.js,clock.css,root.popup.js",
        "treeview" -> "root.treeview.js,treeview.css",
        "confirm" -> "root.popup.js,root.dialog.js",
        "alert" -> "root.popup.js,root.dialog.js",
        "prompt" -> "root.popup.js,root.dialog.js",
        "editor" -> "root.editor.js",
        "editable" -> "root.editor.js",
        "table-datatable" -> "root.datatable.js,datatable.css",
        "textarea-coder" -> "root.coder.js,coder/codemirror.js,coder/codemirror.css,coder.css",
        "a-help" -> "root.help.js,root.popup.js,iconfont.css",
        "a-onclick" -> "root.anchor.js,root.dialog.js,root.callout.js,root.popup.js",
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
        "properties"-> "properties.js"
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
            |       <script type="text/javascript" src="@/root.datetime.js"></script>
            |       <script type="text/javascript" src="@/root.animation.js"></script>
            |       <script type="text/javascript" src="@/root.input.js"></script>
            |       <link href="@/css/root/main.css" rel="stylesheet" type="text/css" />
            |       <link href="@/css/root/iconfont.css" rel="stylesheet" type="text/css" />
            |       #{scripts}
            |   </head>
            |   <body>
            |       <div class="marker-frame">
            |           #{content}
            |       </div>
            |   </body>
            |</html>""".stripMargin

    def getScripts(content: String): String = {
        """(?i)<div[^>]+display=|\bCallout\b|\bfocusView\b|\$cookie\b|<backtop\b|<select\b|\sselect=|<button\b|type="datetime"|<calendar\b|<clock\b|<input[^>]+type="calendar"|<treeview\b|\$root.(confirm|alert|prompt)|<editor\b|\s(editable)=|<table[^>]+datatable="|<textarea[^>]+coder=|<a\b[^>]+help=|<a\b[^>]+onclick\+=|\bpopup=""".r
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
            """<textarea[^>]+coder="([a-z-]+)"""".r
                .findAllMatchIn(content)
                .map(_.group(1).toLowerCase())
                .toSeq
                .distinct
                .map(codes(_))
                .map(js => s"""<script type="text/javascript" src="@/coder/$js"></script>""")
                .mkString("\n")
    }
}