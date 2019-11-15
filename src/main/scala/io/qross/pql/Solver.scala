package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns.{$CONSTANT, $INTERMEDIATE$N, FUNCTION_NAMES, $LINK}

import scala.collection.mutable
import scala.util.matching.Regex

object Solver {

    val ARGUMENT: Regex = """[#&]\{([a-zA-Z0-9_]+)\}""".r  //入参 #{name} 或 &{name}
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
    val JSON$N: Regex = """~json\[(\d+)\]""".r  //JSON占位符
    val VALUE$N: Regex = """~value\[(\d+)\]""".r //中间结果占位符
    val STR$N: Regex = """~str\[(\d+)\]""".r //计算过程中的字符串占位符

    implicit class Sentence$Solver(var sentence: String) {

        //clear comments  --  /* */
        //constants = char, rich string, json -> stash them to avoid conflict
        //close examination  ( ) [ ] { } <% %>
        def cleanCommentsAndStashConstants(PQL: PQL): String = {

            //internal class
            class Block$Range(val name: String, val start: Int, var end: Int = -1) { }
            class Closing(val char: Char, val index: Int) { }

            sentence = sentence.replace("'''", "%three-single-quotes%")
                               .replace("\"\"\"", "%three-double-quotes%")

            val blocks = new mutable.ArrayStack[Block$Range]()
            val closing = new mutable.ArrayStack[Closing]()

            for (i <- sentence.indices) {
                val c = sentence.charAt(i)
                c match {
                    //单行注释开始
                    case '-' =>
                        if (i > 0 && sentence.charAt(i - 1) == '-' && closing.isEmpty) {
                            closing += new Closing('-', i)
                            blocks += new Block$Range("SINGLE-LINE-COMMENT", i - 1)
                        }
                    //单行注释结束
                    case '\r' =>
                        if (closing.nonEmpty && closing.head.char == '-') {
                            blocks.head.end = i + 1
                            closing.pop()
                        }
                    //多行注释开始
                    case '*' =>
                        if (i > 0 && sentence.charAt(i - 1) == '/' && closing.isEmpty) {
                            closing += new Closing('*', i)
                            blocks += new Block$Range("MULTI-LINES-COMMENT", i - 1)
                        }
                    //多行注释结束
                    case '/' =>
                        if (closing.nonEmpty && closing.head.char == '*' && i > 0 && sentence.charAt(i - 1) == '*') {
                            blocks.head.end = i + 1
                            closing.pop()
                        }
                    //单引号字符串
                    case '\'' =>
                        if (closing.isEmpty) {
                            closing += new Closing('\'', i)
                            blocks += new Block$Range("SINGLE-QUOTE-STRING", i)
                        }
                        else if (closing.head.char == '\'') {
                            if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                blocks.head.end = i + 1
                                closing.pop()
                            }
                        }
                    //双引号字符串
                    case '"' =>
                        if (closing.isEmpty) {
                            closing += new Closing('"', i)
                            blocks += new Block$Range("DOUBLE-QUOTE-STRING", i)
                        }
                        else if (closing.head.char == '"') {
                            if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                blocks.head.end = i + 1
                                closing.pop()
                            }
                        }
                    case _ =>
                }
            }

            //最后一行是注释
            if (closing.nonEmpty) {
                val clip = sentence.takeAfter(closing.head.index - 1).take(20)
                closing.head.char match {
                    case '-' => blocks.head.end = sentence.length
                    case '*' => throw new SQLParseException("Multi-lines comment isn't closed. " + clip)
                    case '\'' => throw new SQLParseException("Char isn't closed. " + clip)
                    case '"' => throw new SQLParseException("String isn't closed. " + clip)
                }
                closing.pop()
            }

            blocks.foreach(closed => {
                    val before = sentence.takeBefore(closed.start)
                    val after = sentence.takeAfter(closed.end - 1)
                    val replacement = {
                        if (closed.name == "SINGLE-LINE-COMMENT" || closed.name == "MULTI-LINES-COMMENT") {
                            ""
                        }
                        else {
                            PQL.chars += sentence.substring(closed.start, closed.end)
                            "~char[" + (PQL.chars.size - 1) + "]"
                        }
                    }

                    sentence = before + replacement + after
                })

            sentence = sentence.replace("%three-single-quotes%", "'''")
                               .replace("%three-double-quotes%", "\"\"\"")

            //找出富字符串
            RICH_CHAR.foreach(regex => {
                regex.findAllIn(sentence)
                    .foreach(string => {
                        PQL.strings += string
                        sentence = sentence.replace(string, "~string[" + (PQL.strings.size - 1) + "]")
                    })
            })

            blocks.clear()
            closing.clear()


            for (i <- sentence.indices) {
                val c = sentence.charAt(i)
                c match {
                    //左小括号, 只检查闭合
                    case '(' =>
                        closing += new Closing('(', i)
                    //右小括号, 闭合检查
                    case ')' =>
                        if (closing.nonEmpty && closing.head.char == '(') {
                            closing.pop()
                        }
                        else {
                            throw new SQLParseException("Miss left round bracket '('." + sentence.substring(0, i).takeRight(20))
                        }
                    //左中括号, JSON数组, 闭合检查
                    case '[' =>
                        if (i > 0 && sentence.charAt(i - 1).isLower) {
                            //占位符
                            closing += new Closing('~', i)
                        }
                        else {
                            blocks += new Block$Range("JSON-ARRAY-SQUARE-BRACKET", i)
                            closing += new Closing('[', i)
                        }
                    //右中括号, JSON数组, 闭合检查
                    case ']' =>
                        if (closing.nonEmpty && closing.head.char == '~') {
                            closing.pop()
                        }
                        else if (closing.nonEmpty && closing.head.char == '[') {
                            blocks.head.end = i + 1
                            closing.pop()
                        }
                        else {
                            throw new SQLParseException("Miss left square bracket '['." + sentence.substring(0, i).takeRight(20))
                        }
                    //左大括号, JSON对象, 闭合检查
                    case '{' =>
                        //${ }  ${{ }}  ~{ } ~{{ }}
                        if (i > 0 && sentence.charAt(i - 1) == '$' || sentence.charAt(i - 1) == '~' ) {
                            //嵌入式变量
                            closing += new Closing('$', i)
                        }
                        else if (i > 0 && sentence.charAt(i - 1) == '{') {
                            //${{ 或 ~{{ 第二个括号, 啥也不干, 忽略
                        }
                        else {
                            if (blocks.isEmpty || blocks.head.name != "JSON-ARRAY-SQUARE-BRACKET" || blocks.head.end > -1) {
                                blocks += new Block$Range("JSON-OBJECT-CURLY-BRACKET", i)
                            }
                            closing += new Closing('{', i)
                        }
                    //右大括号, JSON对象, 闭合检查
                    case '}' =>
                        if (i > 0 && sentence.charAt(i - 1) == '}') {
                            //}}的第二个括号, 啥也不做, 忽略
                        }
                        else if (closing.nonEmpty && closing.head.char == '$') {
                            closing.pop()
                        }
                        else if (closing.nonEmpty && closing.head.char == '{') {
                            if (blocks.nonEmpty && blocks.head.name == "JSON-OBJECT-CURLY-BRACKET") {
                                blocks.head.end = i + 1
                            }
                            closing.pop()
                        }
                        else {
                            throw new SQLParseException("Miss left curly bracket '{'." + sentence.substring(0, i).takeRight(20))
                        }
                    //<% %>
                    case '%' =>
                        if (i > 0 && sentence.charAt(i - 1) == '<') {
                            closing += new Closing('%', i)
                        }
                    case '>' =>
                        if (i > 0 && sentence.charAt(i - 1) == '%') {
                            if (closing.nonEmpty && closing.head.char == '%') {
                                closing.pop()
                            }
                            else {
                                throw new SQLParseException("Miss left server bracket '<%'." + sentence.substring(0, i).takeRight(20))
                            }
                        }
                    case _ =>
                }
            }

            if (closing.nonEmpty) {
                val clip = sentence.takeAfter(closing.head.index - 1).take(20)
                closing.head.char match {
                    case '(' => throw new SQLParseException("Round bracket isn't closed. " + clip)
                    case '[' => throw new SQLParseException("Square bracket isn't closed. " + clip)
                    case '$' => throw new SQLParseException("Embedded bracket isn't closed. " + clip)
                    case '{' => throw new SQLParseException("Curly bracket isn't closed. " + clip)
                    case '%' => throw new SQLParseException("Server bracket isn't closed. " + clip)
                }
            }

            blocks.foreach(closed => {
                val before = sentence.takeBefore(closed.start)
                val after = sentence.takeAfter(closed.end - 1)
                val replacement = "~json[" + PQL.jsons.size + "]"
                PQL.jsons += sentence.substring(closed.start, closed.end)

                sentence = before + replacement + after
            })

            sentence.trim
        }

        def restoreJsons(PQL: PQL): String = {
            JSON$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PQL.jsons.size) {
                        sentence = sentence.replace(m.group(0),
                            PQL.jsons(i)
                                .$clean(PQL)
                                .restoreChars(PQL, "\"")
                                .restoreValues(PQL, "\"")
                                .restoreSymbols())
                    }
                })

            sentence
        }

        //恢复字符串
        def restoreChars(PQL: PQL, quote: String = ""): String = {

            CHAR$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PQL.chars.size) {
                        val char = PQL.chars(i)
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
                    if (i < PQL.strings.size) {
                        val string = PQL.strings(i)
                        sentence = sentence.replace(m.group(0),
                            if (string.startsWith("\"\"\"")) {
                                string.$trim("\"\"\"")
                                    .$restore(PQL, "")
                                    .replace("\\", "\\\\")
                                    .replace("\"", "\\\"")
                                    .bracket("\"")
                                    .replace("\r", "~u000d")
                                    .replace("\n", "~u000a")
                            }
                            else if (string.startsWith("'''")) {
                                string.$trim("'''")
                                    .$restore(PQL, "")
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

        def restoreValues(PQL: PQL, quote: String = "'"): String = {
            VALUE$N.findAllMatchIn(sentence)
                .foreach(m => {
                    val whole = m.group(0)
                    val index = m.group(1).toInt
                    val suffix = {
                        val i = sentence.indexOf(whole) + whole.length
                        if (i == sentence.length || sentence.substring(i, i+1) != "!") {
                            ""
                        }
                        else {
                            "!"
                        }

                    }
                    sentence = sentence.replace(whole + suffix, PQL.values(index).mkString(if (suffix == "!") "" else quote))
                })

            sentence
        }

        def popStash(PQL: PQL, quote: String = "'"): String = {
            sentence.restoreJsons(PQL)
                    .restoreChars(PQL, quote)
                    .restoreValues(PQL, quote)
                    .restoreSymbols()
        }

        //替换外部变量
        def replaceArguments(args: (String, String)*): String = {
            replaceArguments(args.toMap)
        }

        //替换外部变量
        def replaceArguments(map: Map[String, String]): String = {
            ARGUMENT
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val whole = m.group(0)
                    val fieldName = m.group(1)

                    if (map.contains(fieldName)) {
                        sentence = sentence.replace(whole, map(fieldName))
                    }
                })

            sentence
        }

        //替换外部变量
        def replaceArguments(row: DataRow): String = {
            ARGUMENT
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val whole = m.group(0)
                    val fieldName = m.group(1)

                    if (row.contains(fieldName)) {
                        sentence = sentence.replace(whole, row.getString(fieldName, "null"))
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
        def replaceVariables(PQL: PQL): String = {
            USER_VARIABLE
                .map(r => r.findAllMatchIn(sentence))
                .flatMap(s => s.toList.sortBy(m => m.group(1)).reverse)
                .foreach(m => {
                    val whole = m.group(0)

                    PQL.findVariable(whole).ifFound(data => {
                        sentence = sentence.replace(whole, PQL.$stash(data))
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
                            PQL.findVariable(whole).ifFound(data => {
                                sentence = sentence.replace(whole, PQL.$stash(data))
                            })
                        }
                    })

            sentence
        }

        def toArgs(PQL: PQL): List[DataCell] = {
            var args = sentence.$trim("(", ")")

            if (args.bracketsWith("[", "]") || args.bracketsWith("{", "}")) {
                args = args.popStash(PQL, "\"")
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
                    arg.$sharp(PQL)
                }).toList
            }
        }

        //解析表达式中的函数
        def replaceFunctions(PQL: PQL, quote: String = "'"): String = {

            while (GLOBAL_FUNCTION.test(sentence)) { //循环防止嵌套
                GLOBAL_FUNCTION
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val funName = m.group(1).trim().toUpperCase()
                        val funArgs = m.group(2).trim().toArgs(PQL)

                        if (FUNCTION_NAMES.contains(funName)) {
                            new GlobalFunction(funName).call(funArgs).ifValid(data => {
                                sentence = sentence.replace(m.group(0), PQL.$stash(data))
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
        def replaceSharpExpressions(PQL: PQL): String = {
            SHARP_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    //已在clean过程中, 不需要clean
                    new Sharp(m.group(1)).execute(PQL).ifValid(data => {
                        sentence = sentence.replace(m.group(0), PQL.$stash(data))
                    })
                })

            sentence
        }

        //js表达式
        def replaceJsExpressions(PQL: PQL): String = {

            JS_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    sentence = sentence.replace(m.group(0), PQL.$stash(m.group(1).popStash(PQL).eval()))
                })

            sentence
        }

        //js语句块
        def replaceJsStatements(PQL: PQL): String = {
            JS_STATEMENT
                .findAllMatchIn(sentence)
                .foreach(m => {
                    sentence = sentence.replace(m.group(0), PQL.$stash(m.group(1).popStash(PQL).call()))
                })

            sentence
        }

        //解析嵌入式查询表达式
        def replaceQueryExpressions(PQL: PQL, quote: String = "'"): String = {
            QUERY_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val query: String = m.group(1).trim() //查询语句
                    val caption: String = m.group(2).trim().toUpperCase

                    sentence = sentence.replace(m.group(0), PQL.$stash(
                        caption match {
                            case "SELECT" => new SELECT(query).select(PQL, express = true)
                            case "PARSE" => new PARSE(query).parse(PQL, express = true)
                            case "INSERT" => new INSERT(query).insert(PQL, express = true)
                            case "DELETE" => new DELETE(query).delete(PQL, express = true)
                            case _ => DataCell(PQL.dh.executeNonQuery(query), DataType.INTEGER)
                        }))
                })

            sentence
        }

        //执行sharp短语句, sharp表达式本身的目的是避免嵌套, 所有本身不支持嵌套
        def $sharp(PQL: PQL, quote: String = "'"): DataCell = {
            //中间变量
            if ($INTERMEDIATE$N.test(sentence)) {
                PQL.values(sentence.$trim("~value[", "]").toInt)
            }
            //常量
            else if ($CONSTANT.test(sentence)) {
                DataCell(sentence, DataType.TEXT)
            }
            //SHARP表达式
            else if ($LINK.test(sentence)) {
                new Sharp(sentence).execute(PQL, quote)
            }
            //最短表达式
            else {
                sentence.popStash(PQL, quote).trim().eval()
            }
        }

        //计算表达式, 不包括查询表达式, $clean的子集, 在计算查询表达式时使用
        def $express(PQL: PQL): String = {
            sentence.replaceVariables(PQL)
                .replaceFunctions(PQL)
                .replaceSharpExpressions(PQL)
                .replaceJsExpressions(PQL)
                .replaceJsStatements(PQL)
        }

        //计算表达式, 但保留字符串和中间值
        //先计算查询表达式的原因是如果子表达式过早的被替换掉, 在解析SHARP表达式时会出现字符串解析冲突
        def $clean(PQL: PQL): String = {
            sentence.replaceQueryExpressions(PQL).$express(PQL)
        }

        //按顺序计算嵌入式表达式、变量和函数
        def $restore(PQL: PQL, quote: String = "'"): String = {
            sentence.$clean(PQL).popStash(PQL, quote)
        }

        //计算出最后结果, 适用于表达式
        def $eval(PQL: PQL): DataCell = {
            sentence.$clean(PQL).$sharp(PQL)
        }
    }
}