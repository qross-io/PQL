import io.qross.sql.Solver.{MULTILINE_COMMENT, SINGLE_LINE_COMMENT, WHOLE_LINE_COMMENT}

import scala.util.matching.Regex


val WHOLE_LINE_COMMENT: Regex = """^--.*$""".r //整行注释
val SINGLE_LINE_COMMENT: Regex = """--.*?(\r|$)""".r //单行注释
val MULTILINE_COMMENT: Regex = """/\*[\s\S]*\*/""".r //多行注释

//PSQL识别字符串代码

var previous: Char = ' '
var quote: Int = -1

for (i <- sentence.indices) {
    val c = sentence.charAt(i)
    if (c == '\'') {
        if (previous == '\'') {
            if (sentence.charAt(i-1) != '\\') {
                PSQL.chars += sentence.substring(quote, i + 1)
                previous = ' '
            }
        }
        else if (previous != '"') {
            previous = '\''
            quote = i
        }
    }
    else if (c == '"') {
        if (previous == '"') {
            if (sentence.charAt(i-1) != '\\') {
                PSQL.chars += sentence.substring(quote, i + 1)
                previous = ' '
            }
        }
        else if (previous != '\'') {
            previous = '"'
            quote = i
        }
    }
}

//去掉单行注释
SQL = SQL.split("\r").map(s => {
    if (WHOLE_LINE_COMMENT.test(s)) {
        ""
    }
    else if (SINGLE_LINE_COMMENT.test(s)) {
        SINGLE_LINE_COMMENT.replaceAllIn(s, "")
    }
    else {
        s
    }
}).mkString("\r").trim()

//替换所有字符串
//#char[n]
for (i <- chars.indices) {
    SQL = SQL.replace(chars(i), s"~char[$i]")
}

//去年多行注释
SQL = MULTILINE_COMMENT.replaceAllIn(SQL, "")