package io.qross.app

object Cogo {

    private val scripts = Map[String, String](
        "popup=" -> "<script type=\"text/javascript\" src=\"@/root.popup.js\"></script>",
        "callout" -> "<script type=\"text/javascript\" src=\"@/root.callout.js\"></script>",
        "focusview" -> "<script type=\"text/javascript\" src=\"@/root.focusview.js\"></script>",
        "cookie" -> "<script type=\"text/javascript\" src=\"@/root.storage.js\"></script>",
        "backtop" -> "<script type=\"text/javascript\" src=\"@/root.backtop.js\"></script>",
        "button-action" ->  "<script type=\"text/javascript\" src=\"@/root.button.js\"></script>",
        "calendar" -> "<script type=\"text/javascript\" src=\"@/root.calendar.js\"></script>\r<link href=\"@/root.calendar.css\" rel=\"stylesheet\" type=\"text/css\" />",
        "clock" -> "<script type=\"text/javascript\" src=\"@/root.clock.js\"></script>\r<link href=\"@/root.clock.css\" rel=\"stylesheet\" type=\"text/css\" />",
        "treeview" -> "<script type=\"text/javascript\" src=\"@/root.treeview.js\"></script>\r<link href=\"@/root.treeview.css\" rel=\"stylesheet\" type=\"text/css\" />",
        "confirm" -> "<script type=\"text/javascript\" src=\"@/root.dialog.js\"></script>",
        "alert" -> "<script type=\"text/javascript\" src=\"@/root.dialog.js\"></script>",
        "prompt" -> "<script type=\"text/javascript\" src=\"@/root.dialog.js\"></script>",
        "select-button" -> "<script type=\"text/javascript\" src=\"@/root.selectbutton.js\"></script>",
        "editor" -> "<script type=\"text/javascript\" src=\"@/root.editor.js\"></script>",
        "editable" -> "<script type=\"text/javascript\" src=\"@/root.editor.js\"></script>",
        "table-datatable" -> "<script type=\"text/javascript\" src=\"@/root.datatable.js\"></script>",
        "textarea-coder" -> "<script type=\"text/javascript\" src=\"@/root.coder.js\"></script>\r<script type=\"text/javascript\" src=\"@/coder/codemirror.js\"></script>\r<script type=\"text/javascript\" src=\"@/coder/sql.js\"></script>\r<link href=\"@/coder/codemirror.css\" rel=\"stylesheet\" type=\"text/css\" />\r<link href=\"@/coder/qross.css\" rel=\"stylesheet\" type=\"text/css\" />"
    )

    private val codes = Map[String, String](
        "pql" -> "<script type=\"text/javascript\" src=\"@/coder/sql.js\"></script>",
        "sh" -> "<script type=\"text/javascript\" src=\"@/coder/shell.js\"></script>",
        "shell" -> "<script type=\"text/javascript\" src=\"@/coder/shell.js\"></script>",
        "java" -> "<script type=\"text/javascript\" src=\"@/coder/clike.js\"></script>",
        "scala" -> "<script type=\"text/javascript\" src=\"@/coder/clike.js\"></script>",
        "python" -> "<script type=\"text/javascript\" src=\"@/coder/python.js\"></script>",
        "xml" -> "<script type=\"text/javascript\" src=\"@/coder/xml.js\"></script>",
        "html" -> "<script type=\"text/javascript\" src=\"@/coder/htmlmixed.js\"></script>",
        "css" -> "<script type=\"text/javascript\" src=\"@/coder/css.js\"></script>",
        "sql" -> "<script type=\"text/javascript\" src=\"@/coder/sql.js\"></script>",
        "csharp" -> "<script type=\"text/javascript\" src=\"@/coder/clike.js\"></script>",
        "javascript" -> "<script type=\"text/javascript\" src=\"@/coder/javascript.js\"></script>",
        "properties"-> "<script type=\"text/javascript\" src=\"@/coder/properties.js\"></script>"
    )

    val template: String =
        """
            |<!DOCTYPE html>
            |<html lang="en">
            |   <head>
            |       <meta charset="UTF-8">
            |       <title>#{title}</title>
            |       <script type="text/javascript" src="/root/root.js"></script>
            |       <script type="text/javascript" src="/root/root.model.js"></script>
            |       <script type="text/javascript" src="/root/root.datetime.js"></script>
            |       <script type="text/javascript" src="/root/root.animation.js"></script>
            |       <link href="/root/root.css" rel="stylesheet" type="text/css" />
            |       <link href="/root/iconfont.css" rel="stylesheet" type="text/css" />
            |       #{scripts}
            |   </head>
            |   <body>
            |       #{content}
            |   </body>
            |</html>""".stripMargin

    def getScripts(content: String): String = {
        """(?i)\bpopup=|\bCallout\(|\bfocusView=|\$cookie\b|<backtop\b|<button\b[^>]+?action\b|<calendar\b|<clock\b|<input[^>]+type="calendar"|<treeview\b|\$root.(confirm|alert|prompt)|<select\b[^>]+button="|<editor\b|\s(editable)=|<table[^>]+datatable="|<textarea[^>]+coder=""".r
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
            .mkString("") +
            """<textarea[^>]+mode="([a-z-]+)"""".r
                .findAllMatchIn(content)
                .map(_.group(1).toLowerCase())
                .toSet
                .map(codes(_))
                .mkString("")
    }
}