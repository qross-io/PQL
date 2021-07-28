package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.exception.{SQLExecuteException, SQLParseException}
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.setting.Language

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.control.Breaks._
import scala.util.matching.Regex.Match
import scala.collection.JavaConverters._

object Solver {

    //val ARGUMENT: Regex = """[#&]\{([a-zA-Z0-9_]+)\}""".r  //入参 #{name} 或 &{name}
    val ARGUMENTS: List[Regex] = List[Regex]("""#\{(\w+)}""".r, """#\{('\w+')}""".r, """#\{("\w+")}""".r)
    //val GLOBAL_VARIABLE: Regex = """@\(?([a-zA-Z0-9_]+)\)?""".r //全局变量 @name 或 @(name)
    //val USER_VARIABLE: Regex = """\$\(?([a-zA-Z0-9_]+)\)?""".r //用户变量  $name 或 $(name)
    val USER_VARIABLE: List[Regex] = List[Regex](
        """\$\((\w+)\)""".r,  //防冲突增加小括号时不考虑函数，因为函数没有防冲突的必要，全局变量同理。属性和索引规则符号前面也没有防冲突的必要
        """\$(\w+)\b(?!\s*(\(|\.|\\|\[|:=))""".r // := 前的变量忽略，函数名忽略
    )
    val GLOBAL_VARIABLE: List[Regex] = List[Regex](
        """@\((\w+)\)""".r,
        """@(\w+)\b(?![(.\[])""".r
    )
    val JOB_VARIABLE: List[Regex] = List[Regex](
        """%\((\w+)\)""".r,
        """%(\w+)\b(?![(.\[])""".r
    )

    val USER_COMPLEX_VARIABLE: Regex = """\$(\w+)([.\[])""".r
    val GLOBAL_COMPLEX_VARIABLE: Regex = """@(\w+)([.\[])""".r
    val JOB_COMPLEX_VARIABLE: Regex = """%(\w+)([.\[])""".r
    //val USE_COMPLEX_VARIABLE: Regex = """(?i)\$([a-z0-9_]+)(\.[a-z0-9_]+|\[[^]]+\])+""".r
    //val GLOBAL_COMPLEX_VARIABLE: Regex = """(?i)@([a-z0-9_]+)(\.[a-z0-9_]+|\[[^]]+\])+""".r

    val EMBEDDED_VARIABLE: Regex = """([$@])\{\s*(\w+)(.\w+)?\s*\}""".r
    val FUNCTION: Regex = """([$@])(\w+)\(""".r //用户函数, 未完成
    //val GLOBAL_FUNCTION: Regex = """@([A-Za-z_]+)\s*\(([^)]*)\)""".r //系统函数
    val SHARP_EXPRESSION: Regex = """(?i)\$\{([^{}]+?)}""".r //Sharp表达式
    val QUERY_EXPRESSION: Regex = """(?i)\$\{\{([\s\S]+?)}}""".r //查询表达式
    val INNER_SENTENCE: Regex = """(?i)\(\s*(SELECT|PARSE|REDIS|FILE|DIR|INSERT|UPDATE|DELETE|REPLACE|IF|CASE)\s""".r  //(SELECT...)

    val RICH_CHAR: List[Regex] = List[Regex]("%rich-string%\"[\\s\\S]*?\"%rich-string%".r, "%rich-string%'[\\s\\S]*?'%rich-string%".r) //富字符串
    val CHAR$N: Regex = """~char\[(\d+)]""".r  //字符串占位符
    val TEXT$N: Regex = """~text\[(\d+)]""".r  //富字符串占位符
    val JSON$N: Regex = """~json\[(\d+)]""".r  //JSON占位符
    val SHARP$N: Regex = """~sharp\[(\d+)]""".r  //Sharp表达式
    val VALUE$N: Regex = """~value\[(\d+)]""".r //中间结果占位符
    val STR$N: Regex = """~str\[(\d+)]""".r //计算过程中的字符串占位符
    val INNER$N: Regex = """~inner\[(\d+)]""".r //IF和CASE短语句中的内部语句

    //清理模式
    val FULL: Int = 0 //完全模式, 默认
    val EXPRESS: Int = 1 //快速模式, 适用于 Query 表达式内部
    val NONE: Int = 2 //不清理, 适用于 Sharp表达式内部

    implicit class Sentence$Solver(var sentence: String) {

        //clear comments  --  /* */
        //constants = char, rich string, json -> stash them to avoid conflict
        //close examination  ( ) [ ] { } <% %>
        def cleanCommentsAndStashConstants(PQL: PQL): String = {

            val blocks = new mutable.ArrayStack[Block$Range]()
            val closing = new mutable.ArrayStack[Closing]()
            val length = sentence.length

            for (i <- sentence.indices) {
                val c = sentence.charAt(i)
                c match {
                    //单行注释开始
                    case '-' =>
                        if (closing.isEmpty
                                && i > 0 && sentence.charAt(i - 1) == '-'
                                && !(i > 2 && sentence.charAt(i - 1) == '-' && sentence.charAt(i - 2) == '!' && sentence.charAt(i - 3) == '<')) {
                            closing += new Closing('-', i)
                            blocks += new Block$Range("SINGLE-LINE-COMMENT", i - 1)
                        }
                    //单行注释结束
                    case '\r' | '\n' =>
                        if (closing.nonEmpty && closing.head.char == '-') {
                            blocks.head.end = i
                            closing.pop()
                        }
                    //多行注释开始
                    case '*' =>
                        if (closing.isEmpty && i > 0 && sentence.charAt(i - 1) == '/') {
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
                        //判断顺序这样是为了应对极端情况，如 ''''''
                        //单引号富字符串第三位，前两个字符都是单引号
                        if (i > 1 && sentence.charAt(i - 1) == '\'' && sentence.charAt(i - 2) == '\'') {
                            if (closing.isEmpty) {
                                if (i == 2 || i > 2 && sentence.charAt(i - 3) != '\'') {
                                    closing += new Closing('&', i)
                                    blocks += new Block$Range("SINGLE-RICH-STRING-", i - 2, i + 1)
                                }
                            }
                            else if (closing.head.char == '&' && i >= closing.head.index + 3) {
                                //单引号富字符串结束 - 必须前两个字符是单引号, 但后面的字符不是单引号, 且索引位置大于开始位置至少3个位置
                                if (i < length - 1 && sentence.charAt(i + 1) != '\'' || i == length - 1) {
                                    blocks += new Block$Range("-SINGLE-RICH-STRING", i - 2, i + 1)
                                    closing.pop()
                                }
                            }
                        }
                        //单引号富字符串第二位，前后两位是单引号
                        else if (i > 0 && i + 1 < length && sentence.charAt(i - 1) == '\'' && sentence.charAt(i + 1) == '\'') {
                            //do nothing
                        }
                        //单引号富字符串第一位, 后面两位是单引号
                        else if (i + 2 < length && sentence.charAt(i + 1) == '\'' && sentence.charAt(i + 2) == '\'') {
                            //do nothing
                        }
                        //单引号字符串
                        else {
                            if (closing.isEmpty || closing.head.char == '{') {
                                closing += new Closing('\'', i)
                                blocks += new Block$Range("SINGLE-QUOTE-STRING", i)
                            }
                            else if (closing.head.char == '\'') {
                                if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                    blocks.head.end = i + 1
                                    closing.pop()
                                }
                            }
                        }
                    //双引号字符串
                    case '"' =>
                        //双引号富字符串第三位，前两个字符都是双引号
                        if (i > 1 && sentence.charAt(i - 1) == '"' && sentence.charAt(i - 2) == '"') {
                            if (closing.isEmpty) {
                                if (i == 2 || i > 2 && sentence.charAt(i - 3) != '"') {
                                    closing += new Closing('%', i)
                                    blocks += new Block$Range("DOUBLE-RICH-STRING-", i - 2, i + 1)
                                }
                            }
                            else if (closing.head.char == '%' && i >= closing.head.index + 3) {
                                //双引号富字符串结束 - 必须前两个字符是双引号, 但后面的字符不是双引号, 且索引位置大于开始位置至少3个位置
                                if (i < length - 1 && sentence.charAt(i + 1) != '"' || i == length - 1) {
                                    blocks += new Block$Range("-DOUBLE-RICH-STRING", i - 2, i + 1)
                                    closing.pop()
                                }
                            }
                        }
                        //双引号富字符串第二位，前后两位是双引号
                        else if (i > 0 && i + 1 < length && sentence.charAt(i - 1) == '"' && sentence.charAt(i + 1) == '"') {
                            //do nothing
                        }
                        //双引号富字符串第一位, 后面两位是双引号
                        else if (i + 2 < length && sentence.charAt(i + 1) == '"' && sentence.charAt(i + 2) == '"') {
                            //do nothing
                        }
                        else {
                            if (closing.isEmpty || closing.head.char == '{') {
                                closing += new Closing('"', i)
                                blocks += new Block$Range("DOUBLE-QUOTE-STRING", i)
                            }
                            else if (closing.head.char == '"') {
                                if (i > 0 && sentence.charAt(i - 1) != '\\') {
                                    blocks.head.end = i + 1
                                    closing.pop()
                                }
                            }
                        }
                    case '{' =>
                        //仅处理在富字符串中的大括号
                        if (i > 0 && sentence.charAt(i - 1) == '$') {
                            if (closing.nonEmpty && (closing.head.char == '&' || closing.head.char == '%')) {
                                closing += new Closing('{', i)
                            }
                        }
                    case '}' =>
                        if (closing.nonEmpty && closing.head.char == '{') {
                            closing.pop()
                        }
                    case _ =>
                }
            }

            //如果最后一行是注释
            if (closing.nonEmpty) {
                //val clip = sentence.takeAfter(closing.head.index - 1).take(20)
                val clip = sentence.substring(closing.head.index).take(20)
                closing.head.char match {
                    case '-' => blocks.head.end = sentence.length
                    case '*' => throw new SQLParseException("Multi-lines comment isn't closed. " + clip)
                    case '\'' | '"' => throw new SQLParseException("String isn't closed. " + clip)
                    case '&' | '%' => throw new SQLParseException("Rich string isn't closed. " + clip)
                    case '{' => throw new SQLParseException("Embedded expression isn't closed. " + clip)
                }
                closing.pop()
            }

            //处理字符串
            blocks.foreach(closed => {
                    val before = sentence.take(closed.start)
                    val after = sentence.substring(closed.end)
                    //val after = sentence.takeAfter(closed.end - 1)
                    val replacement = {
                        closed.name match {
                            case "SINGLE-QUOTE-STRING" | "DOUBLE-QUOTE-STRING" =>
                                PQL.chars += sentence.substring(closed.start, closed.end)
                                "~char[" + (PQL.chars.size - 1) + "]"
                            case "SINGLE-RICH-STRING-" => "%rich-string%'"
                            case "-SINGLE-RICH-STRING" => "'%rich-string%"
                            case "DOUBLE-RICH-STRING-" => "%rich-string%\""
                            case "-DOUBLE-RICH-STRING" => "\"%rich-string%"
                            case _ => ""
                        }
                    }

                    sentence = before + replacement + after
                })

            //处理富字符串
            //因为富字符串中可能嵌套子字符串，造成解析出错，只能单独解析
            RICH_CHAR.foreach(regex => {
                                regex.findAllIn(sentence)
                                    .foreach(string => {
                                        PQL.strings += string
                                        sentence = sentence.replace(string, "~text[" + (PQL.strings.size - 1) + "]")
                                    })
                            })

            blocks.clear()
            closing.clear()

            val ranges = new mutable.ArrayBuffer[Block$Range]()

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
                        if (i > 0 && (sentence.charAt(i - 1).isLower || sentence.charAt(i - 1) == ']')) {
                            //占位符或第一或二索引项
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
                            ranges += blocks.pop()
                        }
                        else {
                            throw new SQLParseException("Miss left square bracket '['." + sentence.substring(0, i).takeRight(20))
                        }
                    //左大括号
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
                            //JSON对象
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
                            closing.pop()
                            if (blocks.nonEmpty && blocks.head.name == "JSON-OBJECT-CURLY-BRACKET") {
                                blocks.head.end = i + 1
                                ranges += blocks.pop()
                            }
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
                //val clip = sentence.takeAfter(closing.head.index - 1).take(20)
                val clip = sentence.substring(closing.head.index).take(20)
                closing.head.char match {
                    case '(' => throw new SQLParseException("Round bracket isn't closed. " + clip)
                    case '[' => throw new SQLParseException("Square bracket isn't closed. " + clip)
                    case '$' => throw new SQLParseException("Embedded bracket isn't closed. " + clip)
                    case '{' => throw new SQLParseException("Curly bracket isn't closed. " + clip)
                    case '%' => throw new SQLParseException("Server bracket isn't closed. " + clip)
                }
            }

            //处理char 160 - 网页端提交的代码很有可能包含这个字符 \u00A0 即 &nbsp;
            sentence = sentence.replace("\u00A0", " ");

            //提取json常量, 量其中可能包含变量和表达式
            val jsons = ranges.map(closed => sentence.substring(closed.start, closed.end)).reverse
            for (i <- jsons.indices) {
                val replacement = "~json[" + PQL.jsons.size + "]"
                PQL.jsons += jsons(i)
                sentence = sentence.replace(jsons(i), replacement)
                if (i + 1 < jsons.length) {
                    jsons(i+1) = jsons(i+1).replace(jsons(i), replacement)
                }
            }

           sentence.trim
        }

        //恢复Json常量
        def restoreJsons(PQL: PQL): String = {
            breakable {
                while (true) {
                    JSON$N.findFirstMatchIn(sentence) match {
                        case Some(m) =>
                            sentence = sentence.replace(m.group(0), PQL.jsons(m.group(1).toInt).$restore(PQL, "\""))
                        case None => break
                    }
                }
            }

            sentence
        }

        //恢复字符串
        def restoreChars(PQL: PQL): String = {

            CHAR$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PQL.chars.size) {
                        sentence = sentence.replace(m.group(0), PQL.chars(i))
                    }
                })

            TEXT$N
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PQL.strings.size) {
                        val string = PQL.strings(i)
                        sentence = sentence.replace(m.group(0),
                            if (string.startsWith("%rich-string%\"")) {
                                string.$trim("%rich-string%\"", "\"%rich-string%")
                                    .$restore(PQL, "")
                                    .replace("\\", "\\\\")
                                    .replace("\"", "\\\"")
                                    .replace("\r", "~u000d")
                                    .replace("\n", "~u000a")
                                    .bracket("\"")
                            }
                            else if (string.startsWith("%rich-string%'")) {
                                string.$trim("%rich-string%'", "'%rich-string%")
                                    .$restore(PQL, "")
                                    .replace("\\", "\\\\")
                                    .replace("'", "\\'")
                                    .replace("\r", "~u000d")
                                    .replace("\n", "~u000a")
                                    .bracket("'")
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
                    .restoreChars(PQL)
                    .restoreValues(PQL, quote)
                    .restoreSymbols()
        }

        def containsArguments: Boolean = {
            ARGUMENTS.flatMap(_.findAllMatchIn(sentence)).nonEmpty
        }

        def findArguments: List[String] = {
            ARGUMENTS.flatMap(_.findAllMatchIn(sentence).map(_.group(0)))
        }

        def replaceArguments(args: String): String = {
            replaceArguments(args.splitToMap())
        }

        //替换外部变量
        def replaceArguments(args: (String, String)*): String = {
            replaceArguments(args.toMap)
        }

        def replaceArguments(args: java.util.Map[String, Object]): String = {
            replaceArguments(args.asScala.map(kv => (kv._1, kv._2.toString)).toMap)
        }

        //替换外部传参
        def replaceArguments(map: Map[String, String]): String = {
            ARGUMENTS
                .flatMap(r => r.findAllMatchIn(sentence))
                .foreach(m => {
                    val whole = m.group(0)
                    val (fieldName, quote) = {
                        if (m.group(1).quotesWith("'")) {
                            (m.group(1).removeQuotes(), "'")
                        }
                        else if (m.group(1).quotesWith("\"")) {
                            (m.group(1).removeQuotes(), "\"")
                        }
                        else {
                            (m.group(1), "")
                        }
                    }

                    if (map.contains(fieldName)) {
                        sentence = sentence.replace(whole, {
                            if (quote == "'") {
                                map(fieldName).preventInjection
                            }
                            else if (quote == "\"") {
                                map(fieldName).preventInjectionOfDoubleQuote
                            }
                            else {
                                map(fieldName)
                            }
                        })
                    }
                })

            sentence
        }

        //替换外部传参
        def replaceArguments(row: DataRow): String = {
            ARGUMENTS
                .flatMap(r => r.findAllMatchIn(sentence))
                .foreach(m => {
                    val whole = m.group(0)
                    val (fieldName, quote) = {
                        if (m.group(1).quotesWith("'")) {
                            (m.group(1).removeQuotes(), "'")
                        }
                        else if (m.group(1).quotesWith("\"")) {
                            (m.group(1).removeQuotes(), "\"")
                        }
                        else {
                            (m.group(1), "")
                        }
                    }

                    if (row.contains(fieldName)) {
                        sentence = sentence.replace(whole,
                            {
                                if (quote == "'") {
                                    row.getString(fieldName, "null").preventInjection
                                }
                                else if (quote == "\"") {
                                    row.getString(fieldName, "null").preventInjectionOfDoubleQuote
                                }
                                else {
                                    row.getString(fieldName, "null")
                                }
                            })
                    }
                })

            sentence
        }

        //替换特殊符号
        def restoreSymbols(): String = {
            sentence.replace("~u003b", ";")
                    .replace("~u0024", "$")
                    .replace("~u0040", "@")
                    //.replace("~u007e", "~") //已去掉javascript嵌入
                    .replace("~u0021", "!")
                    //.replace("~u0023", "#") //已在Parameter中替换
                    //.replace("~u0026", "&")//已在Parameter中替换
                    .replace("~u000d", "\r")
                    .replace("~u000a", "\n")
                    .replace("~u002d", "-")
                    .replace("~u002f", "/")
                    .replace("~u0028", "(")
                    .replace("~u0029", ")")
                    .replace("~u0022", "\"")
                    .replace("~u0027", "'")
        }

        def creep(data: DataCell, m: Match, PQL: PQL): String = {
            val whole = m.group(0)
            var tail = m.group(2)

            val left = sentence.takeBefore(whole)
            var right = sentence.takeAfter(whole)
            var creep = data

            breakable {
                while ((tail == "." || tail == "[") && (creep.isJavaList || creep.isRow || creep.isTable || creep.isExtensionType)) {
                    if (tail == ".") {
                        "^[a-zA-Z0-9_]+".r.findFirstIn(right) match {
                            case Some(attr) =>
                                right = right.substring(attr.length)
                                if (right != "") {
                                    tail = right.take(1)
                                    right = right.drop(1)
                                }
                                else {
                                    tail = ""
                                }

                                creep = creep.getDataByProperty(attr)
                            case None => break
                        }
                    }
                    else if (tail == "[") { // "["
                        val half = right.indexHalfOf('[', ']')
                        if (half > -1) {
                            val index = right.take(half)
                            right = right.substring(index.length + 1)
                            if (right != "") {
                                tail = right.take(1)
                                right = right.drop(1)
                            }
                            else {
                                tail = ""
                            }
                            //仅支持变量和简单运算，不支持嵌套
                            creep = creep.getDataByIndex(index.popStash(PQL))
                        }
                        else {
                            break
                        }
                    }
                }
            }

            left + PQL.$stash(creep) + tail + right
        }

        //解析表达式中的变量
        def replaceVariables(PQL: PQL): String = {

            USER_VARIABLE
                .map(r => r.findAllMatchIn(sentence))
                .flatMap(s => s.toList.sortBy(m => m.group(0)).reverse)  //反转很重要, $user  $username 必须先替换长的
                .foreach(m => {
                    val whole = m.group(0)
                    val name = m.group(1)

                    PQL.findVariable("$" + name)
                        .ifFound(data => {
                            sentence = sentence.replaceFirstOne(whole, PQL.$stash(data))
                        }).ifNotFound(() => {
                            if (PQL.dh.debugging) {
                                Output.writeWarning(s"The variable $$$name has not been assigned.")
                            }
                            //变量未定义
                            sentence = sentence.replaceFirstOne(whole, PQL.$stash(DataCell.UNDEFINED))
                        })
                })

            GLOBAL_VARIABLE
                .map(r => r.findAllMatchIn(sentence))
                .flatMap(s => s.toList.sortBy(m => m.group(0)).reverse)
                .foreach(m => {
                    val whole = m.group(0)
                    val name = m.group(1)

                    PQL.findVariable("@" + name)
                        .ifFound(data => {
                            sentence = sentence.replaceFirstOne(whole, PQL.$stash(data))
                        })
                        .ifNotFound(() => {
                            // @name 标记与MySQL和SQL Server冲突, 不做处理
                            if (PQL.dh.debugging) {
                                Output.writeWarning(s"The global variable @$name maybe has not been assigned.")
                            }
                        })
                })

            USER_COMPLEX_VARIABLE
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val name = m.group(1)

                    PQL.findVariable("$" + name)
                        .ifFound(data => {
                            sentence = sentence.creep(data, m, PQL)
                        })
                        .ifNotFound(() => {
                            if (PQL.dh.debugging) {
                                Output.writeWarning(s"The variable $$$name has not been assigned.")
                            }
                            //变量未定义
                            sentence = sentence.replaceFirstOne(m.group(0), "UNDEFINED" + m.group(2))
                        })
                })

            GLOBAL_COMPLEX_VARIABLE
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val name = m.group(1)

                    PQL.findVariable("@" + name)
                        .ifFound(data => {
                            sentence = sentence.creep(data, m, PQL)
                        })
                        .ifNotFound(() => {
                            // @name 标记与MySQL和SQL Server冲突, 不做处理
                            if (PQL.dh.debugging) {
                                Output.writeWarning(s"The global variable @$name maybe has not been assigned.")
                            }
                        })
                })

            if (PQL.jobId > 0) {
                JOB_VARIABLE
                    .map(r => r.findAllMatchIn(sentence))
                    .flatMap(s => s.toList.sortBy(m => m.group(0)).reverse)
                    .foreach(m => {
                        val whole = m.group(0)
                        val name = m.group(1)

                        PQL.findVariable("%" + name)
                            .ifFound(data => {
                                sentence = sentence.replaceFirstOne(whole, PQL.$stash(data))
                            })
                    })

                JOB_COMPLEX_VARIABLE
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val name = m.group(1)
                        PQL.findVariable("%" + name)
                            .ifFound(data => {
                                sentence = sentence.creep(data, m, PQL)
                            })
                    })
            }

            sentence
        }

        def replaceEmbeddedVariables(PQL: PQL): String = {
            EMBEDDED_VARIABLE.findAllMatchIn(sentence)
                .foreach(m => {
                    val whole = m.group(0)
                    val symbol = m.group(1)
                    val name = m.group(2)

                    PQL.findVariable(symbol + name).ifFound(data => {
                        if (m.group(3) == null) {
                            sentence = sentence.replace(whole, if (data.value == null || data.invalid) "null" else data.value.toString)
                        }
                        else {
                            val name = m.group(3).drop(1)
                            val cell: DataCell = {
                                if (data.isRow) {
                                    data.asRow.getCell(name)
                                }
                                else if (data.isExtensionType) {
                                    try {
                                        Class.forName(data.dataType.typeName)
                                            .getDeclaredMethod("getCell", classOf[String])
                                            .invoke(data.value, name)
                                            .asInstanceOf[DataCell]
                                    }
                                    catch {
                                        case _: Exception => throw new SQLExecuteException("Class " + data.dataType.typeName + " must contains method getCell(String).")
                                    }
                                }
                                else {
                                    DataCell.ERROR
                                }
                            }
                            sentence = sentence.replace(whole, if (cell.value == null) "null" else cell.value.toString)
                        }
                    })
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

        def callFunctions(PQL: PQL, quote: String, head: String, symbol: String, name: String): String = {
            val left = sentence.takeBefore(head)
            var right = sentence.takeAfter(head)
            val half = right.indexHalfOf('(', ')')
            var args = right.take(half) //函数参数
            right = right.drop(half + 1)

            //检查嵌套
            FUNCTION.findFirstMatchIn(args) match {
                case Some(m) => args = args.callFunctions(PQL, quote, m.group(0), m.group(1), m.group(2))
                case None =>
            }

            //执行
            left + PQL.$stash(CALL.call(PQL, symbol, name.toUpperCase(), args)) + right
        }

        //解析表达式中的函数
        def replaceFunctions(PQL: PQL, quote: String = "'"): String = {

            breakable {
                while (true) {
                    FUNCTION.findFirstMatchIn(sentence) match {
                        case Some(m) => sentence = sentence.callFunctions(PQL, quote, m.group(0), m.group(1), m.group(2))
                        case None => break
                    }
                }
            }

            sentence
        }

        //解析嵌入式Sharp表达式
        def replaceSharpExpressions(PQL: PQL): String = {
            SHARP_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    //已在clean过程中, 不需要clean
                    sentence = sentence.replace(m.group(0), PQL.$stash(m.group(1).trim().$compute(PQL, Solver.NONE)))
                    //new Sharp(m.group(1)).execute(PQL).ifValid(data => {
//                      sentence = sentence.replace(m.group(0), PQL.$stash(data))
//                  })
                })

            sentence
        }

        //解析嵌入式查询表达式
        def replaceQueryExpressions(PQL: PQL): String = {
            QUERY_EXPRESSION
                .findAllMatchIn(sentence)
                .foreach(m => {
                    //val query: String = m.group(1).trim() //查询语句
                    //val caption: String = m.group(2).trim().toUpperCase
                    sentence = sentence.replace(m.group(0), PQL.$stash(m.group(1).trim().$compute(PQL, Solver.EXPRESS)))
                })

            sentence
        }

        //执行sharp短语句, sharp表达式本身的目的是避免嵌套, 所有本身不支持嵌套
        def $sharp(PQL: PQL, quote: String = "'"): DataCell = {
            //中间变量
            if ($INTERMEDIATE$N.test(sentence)) {
                PQL.values(sentence.$trim("~value[", "]").toInt)
            }
            else if ($NULL.test(sentence)) {
                DataCell.NULL
            }
            //常量
            else if ($CONSTANT.test(sentence) || $AS.test(sentence)) {
                DataCell(sentence, DataType.TEXT)
            }
            //SHARP表达式
            else if ($LINK.test(sentence)) {
                new Sharp(sentence).execute(PQL)
            }
            else if (sentence.bracketsWith("(", ")") || sentence.bracketsWith("~u0028", "~u0029")) {
                DataCell(sentence.popStash(PQL, quote), DataType.TEXT)
            }
            //最短表达式
            else {
                sentence.popStash(PQL, quote).trim().eval()
            }
        }

        //计算表达式, 不包括查询表达式, 在计算查询表达式时使用
        def $express(PQL: PQL): String = {
            sentence.replaceVariables(PQL)
                    .replaceFunctions(PQL)
                    .replaceSharpExpressions(PQL)
        }

        //计算表达式, 但保留字符串和中间值
        //先计算查询表达式的原因是如果子表达式过早的被替换掉, 在解析SHARP表达式时会出现字符串解析冲突
        def $clean(PQL: PQL): String = {
            sentence.replaceQueryExpressions(PQL)
                    .replaceVariables(PQL)
                    .replaceFunctions(PQL)
                    .replaceSharpExpressions(PQL)
        }

        //按顺序计算嵌入式表达式、变量和函数
        def $restore(PQL: PQL, quote: String = "'"): String = {
            sentence.$clean(PQL).popStash(PQL, quote)
        }

        //计算出最后结果, 适用于表达式
        def $eval(PQL: PQL): DataCell = {
            sentence.$clean(PQL).$sharp(PQL)
        }

        //条件式中的内部语句
        def replaceInnerSentence(PQL: PQL): String = {

            var start: Int = 0
            val groups = new mutable.ArrayStack[Block$Range]()

            INNER_SENTENCE.findAllIn(sentence)
                .foreach(head => {
                    val begin: Int = sentence.indexOf(head, start) + 1
                    if (begin > 0) {
                        var end: Int = sentence.indexOf(")", start)

                        val brackets = new mutable.ArrayStack[String]
                        brackets.push("(")
                        start = begin

                        while (brackets.nonEmpty && sentence.indexOf(")", start) > -1) {
                            val left: Int = sentence.indexOf("(", start)
                            val right: Int = sentence.indexOf(")", start)
                            if (left > -1 && left < right) {
                                brackets.push("(")
                                start = left + 1
                            }
                            else {
                                brackets.pop()
                                start = right + 1
                                if (right > end) end = right
                            }
                        }

                        if (brackets.nonEmpty) {
                            throw new SQLParseException("Can't find closed bracket for SELECT: " + sentence)
                        }
                        else {
                            groups += new Block$Range(head, begin - 1, end + 1)
                        }
                    }
                })

            groups.foreach(block => {
                PQL.inners += sentence.substring(block.start, block.end)
                sentence = sentence.replace(PQL.inners.last, "~inner[" + (PQL.inners.length - 1) + "]")
            })

            sentence
        }

        //恢复一条内部语句
        def restoreInnerSentence(PQL: PQL): String = {
            PQL.inners(sentence.$trim("~inner[", "]").toInt).$trim("(", ")").trim()
        }

        //仅在ConditionGroup中使用, 将 IN () 或 EXISTS () 中的查询语句换成 ~value[n]
        def replaceInnerSentences(PQL: PQL): String = {
            INNER$N.findFirstMatchIn(sentence)
                .foreach(m => {
                    sentence = sentence.replace(m.group(0), PQL.$stash(PQL.inners(m.group(1).toInt).$trim("(", ")").trim().$compute(PQL).toJavaList))
                })

            sentence
        }

        def $process(PQL: PQL, express: Int, handler: String => DataCell, quote: String = "'"): DataCell = {

            var body = {
                express match {
                    case 0 => sentence.$clean(PQL)
                    case 1 => sentence.$express(PQL)
                    case 2 => sentence
                    case _ => sentence.$clean(PQL)
                }
            }

            val links = {
                if (body.contains(ARROW)) {
                    body.takeAfter(ARROW)
                }
                else {
                    ""
                }
            }

            if (links != "") {
                body = body.takeBefore(ARROW).trim()
            }

            val data = handler(body.popStash(PQL, quote))

            if (links != "") {
                new Sharp(links, data).execute(PQL)
            }
            else {
                data
            }
        }

        def $compute(PQL: PQL, express: Int = FULL): DataCell = {
            val caption = sentence.takeBeforeX($BLANK).toUpperCase()
            if (EVALUATIONS.contains(caption)) {
                Class.forName(s"io.qross.pql.$caption")
                    .getDeclaredMethod("evaluate", classOf[PQL], classOf[Int])
                    .invoke(Class.forName(s"io.qross.pql.$caption").getConstructor(classOf[String]).newInstance(sentence), PQL, express.asInstanceOf[java.lang.Integer])
                    .asInstanceOf[DataCell]
            }
            else if (NON_QUERY_CAPTIONS.contains(caption)) {
                new NON$QUERY(sentence).evaluate(PQL, express)
            }
            else {
                //在SHARP表达式内部再恢复字符串和中间值
                new Sharp({
                    express match {
                        case 0 => sentence.$clean(PQL)
                        case 1 => sentence.$express(PQL)
                        case 2 => sentence
                        case _ => sentence.$clean(PQL)
                    }
                }).execute(PQL)
            }
        }
    }
}

class Block$Range(val name: String, val start: Int, var end: Int = -1) {

}

class Closing(val char: Char, val index: Int) {

}