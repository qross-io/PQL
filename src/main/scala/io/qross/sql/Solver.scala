package io.qross.sql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.sql.Patterns.{$CONSTANT, $INTERMEDIATE$N, FUNCTION_NAMES}

import scala.collection.mutable
import scala.util.matching.Regex

object Solver {

    val ARGUMENT: Regex = """(#|&)\{([a-zA-Z0-9_]+)\}""".r  //入参 #{name} 或 &{name}
    //val GLOBAL_VARIABLE: Regex = """@\(?([a-zA-Z0-9_]+)\)?""".r //全局变量 @name 或 @(name)
    //val USER_VARIABLE: Regex = """\$\(?([a-zA-Z0-9_]+)\)?""".r //用户变量  $name 或 $(name)
    val USER_VARIABLE: List[Regex] = List[Regex](
        """\$\(([a-zA-Z0-9_]+)\)""".r,
        """\$([a-zA-Z0-9_]+)""".r
    )
    val GLOBAL_VARIABLE: List[Regex] = List[Regex](
        """@\(([a-zA-Z0-9_]+)\)""".r,
        """@([a-zA-Z0-9_]+)""".r
    )
    val USER_DEFINED_FUNCTION: Regex = """$[a-zA-Z_]+\(\)""".r //用户函数, 未完成
    val GLOBAL_FUNCTION: Regex = """@([A-Za-z_]+)\s*\(([^\)]*)\)""".r //系统函数
    val JS_EXPRESSION: Regex = """\~\{([\s\S]+?)}""".r //js表达式
    val JS_STATEMENT: Regex = """\~\{\{([\s\S]+?)}}""".r// js语句块
    val SHARP_EXPRESSION: Regex = """(?i)\$\{([^{][\s\S]+?)\}""".r //Sharp表达式
    val QUERY_EXPRESSION: Regex = """(?i)\$\{\{\s*((SELECT|DELETE|INSERT|UPDATE|PARSE)\s[\s\S]+?)\}\}""".r //查询表达式

    val RICH_CHAR: List[Regex] = List[Regex]("\"\"\"[\\s\\S]*?\"\"\"".r, "'''[\\s\\S]*?'''".r) //富字符串
    val CHAR$N: Regex = """~char\[(\d+)\]""".r  //字符串占位符
    val STRING$N: Regex = """~string\[(\d+)\]""".r  //富字符串占位符
    val VALUE$N: Regex = """~value\[(\d+)\]($|\s|\S)""".r //中间结果占位符

    implicit class Sentence(var sentence: String) {

        def cleanCommentsAndStashChars(PSQL: PSQL): String = {

            sentence = sentence.replace("'''", "%three-single-quotes%")
                               .replace("\"\"\"", "%three-double-quotes%")

            val blocks = new mutable.ArrayBuffer[(String, Int, Int)]()
            var closing: Char = ' '
            var start = -1
            for (i <- sentence.indices) {
                val c = sentence.charAt(i)
                c match {
                    //单行注释开始
                    case '-' =>
                        if (i > 0 && sentence.charAt(i - 1) == '-' && closing == ' ') {
                            closing = c
                            start = i - 1
                        }
                    //单行注释结束
                    case '\r' =>
                        if (closing == '-') {
                            blocks += (("SINGLE-LINE-COMMENT", start, i + 1))
                            start = -1
                            closing = ' '
                        }
                    //多行注释开始
                    case '*' =>
                        if (i > 0 && sentence.charAt(i - 1) == '/' && closing == ' ') {
                            closing = '*'
                            start = i - 1
                        }
                    //多行注释结束
                    case '/' =>
                        if (closing == '*' && i > 0 && sentence.charAt(i - 1) == '*') {
                            blocks += (("MULTI-LINES-COMMENT", start, i + 1))
                            start = -1
                            closing = ' '
                        }
                    //单引号字符串
                    case '\'' =>
                        if (closing == ' ') {
                            closing = '\''
                            start = i
                        }
                        else if (closing == '\'') {
                            if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                blocks += (("SINGLE-QUOTE-STRING", start, i + 1))
                                start = -1
                                closing = ' '
                            }
                        }
                    //双引号字符串
                    case '"' =>
                        if (closing == ' ') {
                            closing = '"'
                            start = i
                        }
                        else if (closing == '"') {
                            if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                blocks += (("DOUBLE-QUOTE-STRING", start, i + 1))
                                start = -1
                                closing = ' '
                            }
                        }
                    case _ =>
                }
            }

            //最后一行是注释
            if (start > -1) {
                closing match {
                    case '-' => blocks += (("SINGLE-LINE-COMMENT", start, sentence.length))
                    case '*' => throw new SQLParseException("Multi-lines comment isn't closed: " + sentence.takeAfter(start - 1).take(20))
                    case '\'' => throw new SQLParseException("Char isn't closed: " + sentence.takeAfter(start - 1).take(20))
                    case '"' => throw new SQLParseException("String isn't closed: " + sentence.takeAfter(start - 1).take(20))
                }
                start = -1
                closing = ' '
            }

            /* 闭合检查
            case '(' =>
            case ')' =>
            case '{' =>
            case '}' =>
            //<% %>
            case '%' =>
            case '>' =>
            */

            blocks.map(closed => {
                (sentence.substring(closed._2, closed._3),
                    closed._1 match {
                        case "SINGLE-LINE-COMMENT" => ""
                        case "MULTI-LINES-COMMENT" => ""
                        case "SINGLE-QUOTE-STRING" => "~"
                        case "DOUBLE-QUOTE-STRING" => "~"
                        case _ =>
                    })
            }).foreach(repl => {
                if (repl._2 == "") {
                    sentence = sentence.replace(repl._1, "")
                }
                else {
                    PSQL.chars += repl._1
                    sentence = sentence.replace(repl._1, "~char[" + (PSQL.chars.size - 1) + "]")
                }
            })

            sentence = sentence.replace("%three-single-quotes%", "'''")
                               .replace("%three-double-quotes%", "\"\"\"")

            //找出富字符串
            RICH_CHAR.foreach(regex => {
                regex.findAllIn(sentence)
                    .foreach(string => {
                        PSQL.strings += string
                        sentence = sentence.replace(string, "~string[" + (PSQL.strings.size - 1) + "]")
                    })
            })

            sentence
        }

        //恢复字符串
        def restoreChars(PSQL: PSQL, quote: String = ""): String = {

            CHAR$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PSQL.chars.size) {
                        val char = PSQL.chars(i)
                        sentence = sentence.replace(m.group(0),
                            if (quote != "") {
                                if (!char.quotesWith(quote)) {
                                    char.removeQuotes().useQuotes(quote)
                                }
                                else {
                                    char
                                }
                            }
                            else {
                                char
                            })
                    }
                })

            STRING$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PSQL.strings.size) {
                        val string = PSQL.strings(i)
                        sentence = sentence.replace(m.group(0),
                            if (string.startsWith("\"\"\"")) {
                                string.$trim("\"\"\"")
                                    .$restore(PSQL, "")
                                    .replace("\\", "\\\\")
                                    .replace("\"", "\\\"")
                                    .bracket("\"")
                                    .replace("\r", "~u000d")
                                    .replace("\n", "~u000a")
                            }
                            else if (string.startsWith("'''")) {
                                string.$trim("'''")
                                    .$restore(PSQL, "")
                                    .replace("\\", "\\\\")
                                    .replace("'", "\\'")
                                    .bracket("'")
                                    .replace("\r", "~u000d")
                                    .replace("\n", "~u000a")
                            }
                            else {
                                string
                            })
                    }
                })

            sentence
        }

        def restoreValues(PSQL: PSQL, quote: String = "'"): String = {
            VALUE$N.findAllMatchIn(sentence)
                .foreach(m => {
                    val suffix = m.group(2)
                    sentence = sentence.replace(m.group(0), PSQL.values(m.group(1).toInt).mkString(if (suffix == "!") "" else quote) + (if (suffix == "!") "" else suffix))
                })

            sentence
        }

        def popStash(PSQL: PSQL, quote: String = "'"): String = {
            sentence.restoreChars(PSQL, quote)
                    .restoreValues(PSQL, quote)
                    .restoreSymbols()
        }

        //替换外部变量
        def replaceArguments(map: Map[String, String]): String = {
            ARGUMENT
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val whole = m.group(0)
                    val fieldName = m.group(4)
                    val prefix = m.group(1)

                    if (map.contains(fieldName)) {
                        sentence = sentence.replace(whole, prefix + map(fieldName))
                    }
                })

            sentence
        }

        //替换特殊符号
        def restoreSymbols(): String = {
            sentence.replace("~u003b", ";")
                    .replace("~u0024", "$")
                    .replace("~u0040", "@")
                    .replace("~u007e", "~")
                    .replace("~u0021", "!")
                    //.replace("~u0023", "#") //已在Parameter中替换
                    //.replace("~u0026", "&")//已在Parameter中替换
                    .replace("~u000d", "\r")
                    .replace("~u000a", "\n")
                    .replace("~u002d", "-")
                    .replace("~u002f", "/")
        }

        //解析表达式中的变量
        def replaceVariables(PSQL: PSQL): String = {
            USER_VARIABLE
                .map(r => r.findAllMatchIn(sentence))
                .flatMap(s => s.toList.sortBy(m => m.group(1)).reverse)
                .foreach(m => {
                    val whole = m.group(0)

                    PSQL.findVariable(whole).ifFound(data => {
                        sentence = sentence.replace(whole, PSQL.$stash(data))
                    }).ifNotFound(() => {
                        Output.writeWarning(s"The variable $whole has not been assigned.")
                    })
                })

            GLOBAL_VARIABLE
                    .map(r => r.findAllMatchIn(sentence))
                    .flatMap(s => s.toList.sortBy(m => m.group(1)).reverse)
                    .foreach(m => {
                        val whole = m.group(0)
                        val name = m.group(1)

                        if (whole.contains("(") && whole.endsWith(")") || !FUNCTION_NAMES.contains(name.toUpperCase())) {
                            PSQL.findVariable(whole).ifFound(data => {
                                sentence = sentence.replace(whole, PSQL.$stash(data))
                            })
                        }
                    })

            sentence
        }

        def toArgs(PSQL: PSQL): List[DataCell] = {
            var args = sentence.$trim("(", ")")

            if (args.bracketsWith("[", "]") || args.bracketsWith("{", "}")) {
                args = args.popStash(PSQL, "\"")
                if (Json.OBJECT$ARRAY.test(args)) {
                    List[DataCell](DataCell(Json(args).parseTable("/"), DataType.TABLE))
                }
                else if (args.bracketsWith("{", "}")) {
                    List[DataCell](DataCell(Json(args).parseRow("/"), DataType.ROW))
                }
                else if (args.bracketsWith("[", "]")) {
                    List[DataCell](DataCell(Json(args).parseJavaList("/"), DataType.ARRAY))
                }
                else {
                    List[DataCell]()
                }
            }
            else {
                args.split(",").map(arg => {
                    arg.popStash(PSQL).$sharp(PSQL)
                }).toList
            }
        }

        //解析表达式中的函数
        def replaceFunctions(PSQL: PSQL, quote: String = "'"): String = {

            while (GLOBAL_FUNCTION.test(sentence)) { //循环防止嵌套
                GLOBAL_FUNCTION
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val funName = m.group(1).trim().toUpperCase()
                        val funArgs = m.group(2).trim().toArgs(PSQL)

                        if (FUNCTION_NAMES.contains(funName)) {
                            new Function(funName).call(funArgs).ifValid(data => {
                                sentence = sentence.replace(m.group(0), PSQL.$stash(data))
                            })
                        }
                        else {
                            throw new SQLExecuteException(s"Wrong function name $funName")
                        }
                    })
            }

            sentence
        }

        //解析嵌入式Sharp表达式
        def replaceSharpExpressions(PSQL: PSQL): String = {
            SHARP_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    new SHARP(m.group(1)).execute(PSQL).ifValid(data => {
                        sentence = sentence.replace(m.group(0), PSQL.$stash(data))
                    })
                })

            sentence
        }

        //js表达式
        def replaceJsExpressions(PSQL: PSQL): String = {

            JS_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    sentence = sentence.replace(m.group(0), PSQL.$stash(m.group(1).popStash(PSQL).eval()))
                })

            sentence
        }

        //js语句块
        def replaceJsStatements(PSQL: PSQL): String = {
            JS_STATEMENT
                .findAllMatchIn(sentence)
                .foreach(m => {
                    sentence = sentence.replace(m.group(0), PSQL.$stash(m.group(1).popStash(PSQL).call()))
                })

            sentence
        }

        //解析嵌入式查询表达式
        def replaceQueryExpressions(PSQL: PSQL, quote: String = "'"): String = {
            QUERY_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val query: String = m.group(2).trim() //查询语句
                    val caption: String = m.group(3).trim().toUpperCase

                    sentence = sentence.replace(m.group(0), PSQL.$stash(
                        caption match {
                            case "SELECT" => new SELECT(query).execute(PSQL)
                            case "PARSE" => new PARSE(query.takeAfter("""^PARSE\s""".r).eval().asText).execute(PSQL)
                            case _ => DataCell(PSQL.dh.executeNonQuery(query), DataType.INTEGER)
                        }))
                })

            sentence
        }

        //执行sharp短语句, sharp表达式本身的目的是避免嵌套, 所有本身不支持嵌套
        def $sharp(PSQL: PSQL, quote: String = "'"): DataCell = {
            sentence = sentence.restoreChars(PSQL, quote).trim()
            //如果是中间变量
            if ($INTERMEDIATE$N.test(sentence)) {
                PSQL.values(sentence.$trim("~value[", "]").toInt)
            }
            //如果是常量
            else if ($CONSTANT.test(sentence)) {
                DataCell(sentence, DataType.TEXT)
            }
            //如果是最短表达式
            else {
                sentence.restoreValues(PSQL, quote).eval()
            }
        }

        //计算表达式, 但保留字符串和中间值
        def $clean(PSQL: PSQL): String = {
            sentence.replaceVariables(PSQL)
                .replaceFunctions(PSQL)
                .replaceSharpExpressions(PSQL)
                .replaceJsExpressions(PSQL)
                .replaceJsStatements(PSQL)
                .replaceQueryExpressions(PSQL)
        }

        //按顺序计算嵌入式表达式、变量和函数
        def $restore(PSQL: PSQL, quote: String = "'"): String = {
            sentence.$clean(PSQL).popStash(PSQL, quote)
        }

        //计算出最后结果, 适用于表达式
        def $eval(PSQL: PSQL): DataCell = {
            sentence.$clean(PSQL).$sharp(PSQL)
        }
    }
}