package io.qross.sql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.sql.Patterns.{FUNCTION_NAMES, $INTERMEDIATE$N}
import io.qross.time.DateTime

import scala.util.matching.Regex

object Solver {

    val SINGLE_LINE_COMMENT: Regex = """--.*(\r|$)""".r //单行注释
    val MULTILINE_COMMENT: Regex = """/\*[\s\S]*\*/""".r //多行注释
    val ARGUMENT: Regex = """(#|&)\{([a-zA-Z0-9_]+)\}""".r  //入参 #{name} 或 &{name}
    val GLOBAL_VARIABLE: Regex = """@\(?([a-zA-Z0-9_]+)\)?""".r //全局变量 @name 或 @(name)
    val USER_VARIABLE: Regex = """\$\(?([a-zA-Z0-9_]+)\)?""".r //用户变量  $name 或 $(name)
    val USER_DEFINED_FUNCTION: Regex = """$[a-zA-Z_]+\(\)""".r //用户函数, 未完成
    val SYSTEM_FUNCTION: Regex = """@([a-z_]+)\(([^\)]*)\)""".r //系统函数, 未完成
    val JS_EXPRESSION: Regex = """\~\{([\s\S]+?)}""".r //js表达式
    val JS_STATEMENT: Regex = """\~\{\{([\s\S]+?)}}""".r// js语句块
    val SHARP_EXPRESSION: Regex = """(?i)\$\{([^{][\s\S]+?)\}""".r //Sharp表达式
    val QUERY_EXPRESSION: Regex = """(?i)\$\{\{\s*((SELECT|DELETE|INSERT|UPDATE|PARSE)\s[\s\S]+?)\}\}""".r //查询表达式

    val RICH_CHAR: List[Regex] = List[Regex]("\"\"\"[\\s\\S]*?\"\"\"".r, "'''[\\s\\S]*?'''".r) //富字符串
    val CHAR$N: Regex = """~char\[(\d+)\]""".r  //字符串占位符
    val VALUE$N: Regex = """~value\[(\d+)\]($|\s|\S)""".r //中间结果占位符

    implicit class Sentence(var sentence: String) {
        //恢复字符串
        def restoreChars(PSQL: PSQL, quote: String = ""): String = {

            CHAR$N
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
                            else if (quote != "") {
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

            sentence
        }

        def restoreValues(PSQL: PSQL, quote: String = "'"): String = {
            VALUE$N.findAllMatchIn(sentence)
                .foreach(m => {
                    val suffix = m.group(2)
                    sentence = sentence.replace(m.group(0), PSQL.values(m.group(1).toInt).getString(if (suffix == "!") "" else quote) + (if (suffix == "!") "" else suffix))
                })

            sentence
        }

        def popStash(PSQL: PSQL, quote: String = "'"): String = {
            sentence.restoreChars(PSQL, quote).restoreValues(PSQL, quote)
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
            sentence.replace("~u0024", "$")
                    .replace("~u0040", "@")
                    .replace("~u007e", "~")
                    .replace("~u0021", "!")
                    .replace("~u0023", "#")
                    .replace("~u003b", ";")
                    .replace("~u0026", "&")
        }

        //解析表达式中的变量
        def replaceVariables(PSQL: PSQL): String = {
            List(GLOBAL_VARIABLE, USER_VARIABLE)
                .map(r => r.findAllMatchIn(sentence))
                .flatMap(s => s.toList.sortBy(m => m.group(1).reverse))
                .foreach(m => {
                    PSQL.findVariable(m.group(0)).ifValid(data => {
                        sentence = sentence.replace(m.group(0), PSQL.$stash(data))
                    })
                })

            sentence
        }

        def toArgs(PSQL: PSQL): List[DataCell] = {
            val args = sentence.popStash(PSQL, "\"").$trim("(", ")")
            if (Json.OBJECT$ARRAY.test(args)) {
                List[DataCell](DataCell(Json(args).parseTable("/"), DataType.TABLE))
            }
            else if (args.bracketsWith("{", "}")) {
                List[DataCell](DataCell(Json(args).parseRow("/")))
            }
            else if (args != "") {
                Json(if (!args.bracketsWith("[", "]")) {
                    args.bracket("[", "]")
                }
                else {
                    args
                }).parseDataCellList("/")
            }
            else {
                List[DataCell]()
            }
        }

        //解析表达式中的函数
        def replaceFunctions(PSQL: PSQL, quote: String = "'"): String = {

            while (SYSTEM_FUNCTION.test(sentence)) { //循环防止嵌套
                SYSTEM_FUNCTION
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
        def $sharp(PSQL: PSQL): DataCell = {
            sentence = sentence.restoreChars(PSQL).trim()
            //如果是中间变量
            if ($INTERMEDIATE$N.test(sentence)) {
                PSQL.values($INTERMEDIATE$N.findAllMatchIn(sentence)
                    .map(m => m.group(1).toInt)
                    .toList.head)
            }
            //如果是括号包围的表达式- 这种情况应用不存在
//            else if (sentence.bracketsWith("(", ")")) {
//                new SHARP(sentence.restoreValues(PSQL)).execute(PSQL)
//            }
            //如果是最短表达式
            else {
                sentence.restoreValues(PSQL).eval()
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

        //仅适用于Json表达式, 保留双引号
        def $place(PSQL: PSQL): String = {
            sentence.$restore(PSQL, "\"")
        }

        //计算出最后结果, 适用于表达式
        def $eval(PSQL: PSQL): DataCell = {
            sentence.$clean(PSQL).$sharp(PSQL)
        }
    }
}