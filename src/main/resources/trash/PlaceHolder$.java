package io.qross.ext;

object PlaceHolder {

    val SINGLE_LINE_COMMENT: Regex = """--.*(\r|$)""".r //单行注释
    val MULTILINE_COMMENT: Regex = """/\*[\s\S]*\*/""".r //多行注释
    val ARGUMENT: Regex = """(#|&)\{([a-zA-Z0-9_]+)\}""".r  //入参 #{name} 或 &{name}
    val GLOBAL_VARIABLE: Regex = """@\(?([a-zA-Z0-9_]+)\)?($|\s|\S)""".r //全局变量 @name 或 @(name)
    val USER_VARIABLE: Regex = """\$\(?([a-zA-Z0-9_]+)\)?($|\s|\S)""".r //用户变量  $name 或 $(name)
    val USER_DEFINED_FUNCTION: Regex = """$[a-zA-Z_]+\(\)($|\s|\S)""".r //用户函数, 未完成
    val SYSTEM_FUNCTION: Regex = """@([a-z_]+)\(([^\)]*)\)($|\s|\S)""".r //系统函数, 未完成
    val JS_EXPRESSION: Regex = """\~\{([\s\S]+?)}($|\s|\S)""".r //js表达式
    val JS_STATEMENT: Regex = """\~\{\{([\s\S]+?)}}($|\s|\S)""".r// js语句块
    val SHARP_EXPRESSION: Regex = """(?i)\$\{\s*LET\s([\s\S]+?)\}($|\s|\S)""".r //Sharp表达式
    val QUERY_EXPRESSION: Regex = """(?i)\$(TABLE|ROW|MAP|OBJECT|ARRAY|LIST|VALUE)?\{\{\s*((SELECT|DELETE|INSERT|UPDATE|PARSE)\s[\s\S]+?)\}\}($|\s|\S)""".r //查询表达式

    val RICH_CHAR: List[Regex] = List[Regex]("\"\"\"[\\s\\S]*?\"\"\"".r, "'''[\\s\\S]*?'''".r) //富字符串
    val $CHAR: Regex = """#char\[(\d+)\]""".r  //

    implicit class Sentence(var sentence: String) {

        //匹配的几个问题
        //先替换长字符匹配，再替换短字符匹配，如 #user 和 #username, 应先替换 #username，再替换 #user
        //原生特殊字符处理，如输出#，则使用两个重复的##

        def clearComment(): String = {
            sentence = SINGLE_LINE_COMMENT.replaceAllIn(sentence, "")
            sentence = MULTILINE_COMMENT.replaceAllIn(sentence, "")
            sentence
        }

        def pickOutChars(): List[String] = {

            val chars = new ListBuffer[String]() //所有字符串

            RICH_CHAR.foreach(regex => {
                regex.findAllIn(sentence).foreach(char => chars += char)
            })

            //找出所有的字符串
            var quote: Int = -1
            var previous: Char = ' '
            for (i <- sentence.indices) {
                val c = sentence.charAt(i)
                if (c == '\'') {
                    if (previous == '\'') {
                        if (sentence.charAt(i-1) != '\\') {
                            chars += sentence.substring(quote, i + 1)
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
                            chars += sentence.substring(quote, i + 1)
                            previous = ' '
                        }
                    }
                    else if (previous != '\'') {
                        previous = '"'
                        quote = i
                    }
                }
            }

            chars.toList
        }

        def stashChars(PSQL: PSQL): String = {
            //#char[n]
            for (i <- PSQL.chars.indices) {
                sentence = sentence.replace(PSQL.chars(i), s"#char[$i]")
            }
            sentence
        }

        //恢复字符串
        def restoreChars(PSQL: PSQL): String = {

            $CHAR
                .findAllMatchIn(sentence)
                .foreach(m => {
                    val i = m.group(1).toInt
                    if (i < PSQL.chars.size) {
                        val char = PSQL.chars(i)
                        sentence = sentence.replace(m.group(0),
                                                        if (char.startsWith("\"\"\"")) {
                                                            char.replace("\"\"\"", "\"").$restore(PSQL, "")
                                                        }
                                                        else if (char.startsWith("'''")) {
                                                            char.replace("'''", "'").$restore(PSQL, "")
                                                        }
                                                        else {
                                                            char
                                                        })
                    }
                })

            sentence
        }

        //替换外部变量
        def replaceArguments(map: Map[String, String]): String = {
            ARGUMENT
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val whole = m.group(0)
                        val fieldName = m.group(4)
                        //val symbol = m.group(3)
                        val prefix = m.group(1)

                        if (map.contains(fieldName)) {
                            sentence = sentence.replace(whole, prefix + map(fieldName))
                        }
                    })

            sentence
        }

        //替换特殊符号 $ 和 @
        def replaceSymbols(): String = {
            sentence = sentence.replace("~u0024", "$")
                                .replace("~u0040", "@")
                                .replace("~u007e", "~")
            sentence
        }

        //解析表达式中的变量
        def replaceVariables(PSQL: PSQL, quote: String = "'"): String = {

            GLOBAL_VARIABLE
                    .findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(1))
                    .reverse
                    .foreach(m => {
                        val whole = m.group(0)
                        val fieldName = m.group(1)
                        val suffix = m.group(2)

                        val value = GlobalVariable.get(fieldName, PSQL)
                        if (value.found) {
                            sentence = sentence.replace(whole, value.getString(if (suffix != "!") quote else "") + suffix)
                        }
                    })

            USER_VARIABLE
                    .findAllMatchIn(sentence)
                    .toList
                    .sortBy(m => m.group(1))
                    .reverse
                    .foreach(m => {
                        val whole = m.group(0)
                        val fieldName = m.group(1)
                        val suffix = m.group(2)

                        val value = PSQL.findVariable(fieldName)
                        if (value.found) {
                            sentence = sentence.replace(whole, value.getString(if (suffix != "!") quote else "") + suffix)
                        }
                    })

            sentence
        }

        //解析表达式中的函数
        def replaceFunctions(PSQL: PSQL, quote: String = "'"): String = {

            while (SYSTEM_FUNCTION.test(sentence)) { //循环防止嵌套
                SYSTEM_FUNCTION
                        .findAllMatchIn(sentence)
                        .foreach(m => {
                            val funName = m.group(1).trim().toUpperCase()
                            val funArgs = m.group(2).split(",")
                            val suffix = m.group(3)

                            if (FUNCTION_NAMES.contains(funName)) {
                                sentence = sentence.replace(m.group(0),
                                    new Function(funName).call(funArgs, PSQL).getString(if (suffix != "!") quote else "") + suffix)
                            }
                            else {
                                throw new SQLExecuteException(s"Wrong function name $funName")
                            }
                        })
            }

            sentence
        }

        //js表达式
        def replaceJsExpressions(quote: String = "'"): String = {

            JS_EXPRESSION
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val suffix = m.group(2)
                        sentence = sentence.replace(m.group(0), m.group(1).eval().getString(if (suffix != "!") quote else "") + suffix)
                    })

            sentence
        }

        //js语句块
        def replaceJsStatements(quote: String = "'"): String = {
            JS_STATEMENT
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val suffix = m.group(2)
                        sentence = sentence.replace(m.group(0), m.group(1).call().getString(if (suffix != "!") quote else "") + suffix)
                    })

            sentence
        }

        //解析嵌入式Sharp表达式
        def replaceSharpExpressions(quote: String = "'"): String = {
            SHARP_EXPRESSION
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        val whole = m.group(0)
                        val expression = m.group(1)
                        val suffix = m.group(3)

                        sentence = sentence.replace(whole, new SHARP(expression).execute() match {
                            case str: String => str.userQuotesIf(quote, suffix != "!")
                            case dt: DateTime => dt.toString.userQuotesIf(quote, suffix != "!")
                            case null => "null"
                            case o => o.toString
                        })
                    })

            sentence
        }

        //解析嵌入式查询表达式
        def replaceQueryExpressions(PSQL: PSQL, quote: String = "'"): String = {
            QUERY_EXPRESSION
                    .findAllMatchIn(sentence)
                    .foreach(m => {
                        var output: String = m.group(1)  //类型
                        var query: String = m.group(2).trim() //查询语句
                        val caption: String = m.group(3).trim().toUpperCase
                        val suffix: String = m.group(4)

                        if (output == null) {
                            output = "AUTO"
                        }
                        else {
                            output = output.trim().toUpperCase()
                        }

                        sentence = sentence.replace(m.group(0),
                                                caption match {
                                                    case "SELECT" =>
                                                        output match {
                                                            case "TABLE" | "AUTO" => PSQL.dh.executeDataTable(query).toString
                                                            case "ROW" | "MAP" | "OBJECT" => PSQL.dh.executeDataTable(query).firstRow.getOrElse(DataRow()).toString
                                                            case "LIST" | "ARRAY" => Json.serialize(PSQL.dh.executeSingleList(query))
                                                            case "VALUE" => PSQL.dh.executeSingleValue(query) match {
                                                                                case Some(v) => v match {
                                                                                                    case str: String => str.userQuotesIf(quote, suffix != "!")
                                                                                                    case dt: DateTime => dt.toString.userQuotesIf(quote, suffix != "!")
                                                                                                    case other => other.toString
                                                                                                }
                                                                                case None => "null"
                                                                            }
                                                        }
                                                    case "PARSE" =>
                                                        query = query.takeAfter("""^PARSE\s""".r).trim.removeQuotes()
                                                        output match {
                                                            case "TABLE" => PSQL.dh.parseTable(query).toString
                                                            case "ROW" | "MAP" | "OBJECT" => PSQL.dh.parseRow(query).toString
                                                            case "LIST" | "ARRAY" => Json.serialize(PSQL.dh.parseList(query))
                                                            case "VALUE" => PSQL.dh.parseValue(query) match {
                                                                                case str: String => str.userQuotesIf(quote, suffix != "!")
                                                                                case dt: DateTime => dt.toString.userQuotesIf(quote, suffix != "!")
                                                                                case other => other.toString
                                                                            }
                                                            case "AUTO" =>
                                                                val result = PSQL.dh.parseNode(query)
                                                                if (result != null) {
                                                                    result.toString
                                                                }
                                                                else {
                                                                    "null"
                                                                }
                                                        }
                                                    //ignore output type
                                                    case _ => PSQL.dh.executeNonQuery(query).toString
                                                })
                    })

            sentence
        }

        //计算嵌入式表达式、变量和函数
        def $restore(PSQL: PSQL, quote: String = "'"): String = {

            sentence = sentence.replaceVariables(PSQL, quote)
                            .replaceFunctions(PSQL, quote)
                            .replaceJsExpressions(quote)
                            .replaceSharpExpressions(quote)
                            .replaceJsExpressions(quote)  //目的是可以和Sharp表达式互相嵌套
                            .replaceJsStatements(quote)
                            .replaceQueryExpressions(PSQL, quote)
                            .restoreChars(PSQL)

            sentence
        }

        //仅适用于Json表达式
        def $place1(PSQL: PSQL): String = {
            $restore(PSQL, "\"")
        }

        //仅计算
        def $compute(PSQL: PSQL): DataCell = {
            sentence = sentence.$restore(PSQL)
            sentence.eval()
        }

        //计算出最后结果, 适用于表达式
        def $eval(PSQL: PSQL): String = {
            sentence = sentence.$compute(PSQL).getString("'")
            sentence
        }
    }
}
