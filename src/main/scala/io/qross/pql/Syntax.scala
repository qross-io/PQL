package io.qross.pql

import io.qross.exception.SQLParseException

import scala.collection.mutable
import io.qross.pql.Patterns._
import io.qross.ext.TypeExt._
import io.qross.fs.TextFile
import scala.util.control.Breaks._

object Syntax {

    val HASH = " # "

    //caption, group # phrase, args
    val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, Int]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()
    //caption, group, phrase, arg
    //val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Int]]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Int]]]()

    val sentences: String = """
            OPEN  [JDBC-DataSource]  AS [alias]  USE [database];
            OPEN  QROSS  USE [database];
            OPEN  DEFAULT  AS [alias]  USE [database];
            OPEN  DATABASE [connectionString]  USERNAME [username]  PASSWORD [password]  DRIVER [driver]  AS [connectionName]  USE [database];
            OPEN  CACHE;
            OPEN  TEMP;
            OPEN  REDIS [host]  SELECT [database];
            OPEN  EXCEL [fileName]  AS [alias];
            OPEN  JSON FILE [fileName]  AS TABLE? [tableName];
            OPEN  CSV FILE [fileName]  AS TABLE? [tableName]  WITH FIRST ROW HEADERS  (id INT, name TEXT, ...)  SKIP [amount];
            OPEN  TXT FILE [fileName]  AS TABLE? [tableName]  WITH FIRST ROW HEADERS  (id INT, name TEXT, ...)  BRACKETED BY m,n  DELIMITED BY 'delimiter'  SKIP [amount];
            OPEN  GZ FILE [fileName]  AS TABLE? [tableName]  WITH FIRST ROW HEADERS  (id INT, name TEXT, ...)  BRACKETED BY m,n  DELIMITED BY 'delimiter'  SKIP [amount];

            SAVE AS  [JDBC-DataSource]  AS [alias]  USE [databaseName];
            SAVE AS  DEFAULT  AS [alias];
            SAVE AS  QROSS  AS [alias];
            SAVE AS  DATABASE [connectionString]  USERNAME [username]  PASSWORD [password]  DRIVER [driver]  AS [connectionName]  USE [database];
            SAVE AS  CACHE  AS [alias];
            SAVE AS  CACHE TABLE [tableName]  PRIMARY KEY [columnName];
            SAVE AS  TEMP  AS [alias];
            SAVE AS  TEMP TABLE [tableName]  PRIMARY KEY [columnName]  UNIQUE KEY (column1, column2, ...)  KEY (column1, column2, ...);
            SAVE AS  REDIS [host]  SELECT [database];
            SAVE AS  NEW? CSV FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...)*;
            SAVE AS  CSV STREAM FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...)*;
            SAVE AS  NEW? TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (header1, column2 AS header2, ...)*;
            SAVE AS  TXT? STREAM FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (header1, column2 AS header2, ...)*;
            SAVE AS  NEW? JSON FILE [fileName];
            SAVE AS  JSON STREAM FILE [fileName];
            SAVE AS  NEW? EXCEL [fileName]  AS [alias]  USE? DEFAULT TEMPLATE  USE? TEMPLATE [templateName];
            SAVE AS  EXCEL STREAM FILE [fileName]  AS [alias]  USE? DEFAULT TEMPLATE  USE? TEMPLATE [templateName];

            SAVE TO  [JDBC-DataSource]  AS [alias]  USE [databaseName];
            SAVE TO  DEFAULT  AS [alias];
            SAVE TO  QROSS  AS [alias];
            SAVE TO  DATABASE [connectionString]  USERNAME [username]  PASSWORD [password]  DRIVER [driver]  AS [connectionName]  USE [database];
            SAVE TO  CACHE  AS [alias];
            SAVE TO  TEMP  AS [alias];
            SAVE TO  REDIS [host]  SELECT [database];
            SAVE TO  NEW? CSV FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...)*;
            SAVE TO  CSV STREAM FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...)*;
            SAVE TO  NEW? TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (header1, column2 AS header2, ...)*;
            SAVE TO  TXT? STREAM FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (header1, column2 AS header2, ...)*;
            SAVE TO  NEW? JSON FILE [fileName];
            SAVE TO  JSON STREAM FILE [fileName];
            SAVE TO  NEW? EXCEL [fileName]  AS [alias]  USE? DEFAULT TEMPLATE  USE? TEMPLATE [templateName];
            SAVE TO  EXCEL STREAM FILE [fileName]  AS [alias]  USE? DEFAULT TEMPLATE  USE? TEMPLATE [templateName];

            BLOCK  FROM [startId]  TO [endId]  PER [blockSize];

            SEND  E?MAIL [title]
                SET? SMTP HOST [host]  SET? PORT [25]
                FROM [personal/sender@domain.com/personal<sedner@domain.com>]
                SET? PERSONAL [name]
                SET? PASSWORD [password]
                ENABLE? SSL [yes/no]
                SET? LANGUAGE [chinese/english]
                USE? DEFAULT TEMPLATE
                USE? TEMPLATE [name/template.html]
                WITH? DEFAULT SIGNATURE
                WITH? SIGNATURE [name,signature.html]
                SET? CONTENT [content/<html>]
                PLACE? DATA { "holder1": "value1", "holder2": "value2", ...}
                ATTACH file1.txt,file2.xlsx,*
                TO personal,sender@domain.com,personal<sedner@domain.com>,*
                CC *
                BCC *;

            REQUEST  JSON API [url]  USE? METHOD [GET/POST/PUT/DELETE]  SEND? DATA {"data": "value"}  SET? HEADER {"name": "value"};
            PARSE  [path]  AS TABLE;
            PARSE  [path]  AS ROW;
            PARSE  [path]  AS OBJECT;
            PARSE  [path]  AS LIST;
            PARSE  [path]  AS ARRAY;
            PARSE  [path]  AS VALUE;

            INSERT  INTO [sheetName]  ROW [startRow]  (A, B, C, ...)  VALUES (value1, value2, ...), (value1, value2, ...);
            DROP  SHEET [sheetName];

            LOAD  PROPERTIES [/conf.properties];
            LOAD  YAML [/conf.yml];
            LOAD  YML [/conf.yml];
            LOAD  JSON CONFIG [/conf.json];
            LOAD  PROPERTIES FROM NACOS [host:port:group:data-id];
            LOAD  YAML FROM NACOS [host:port:group:data-id];
            LOAD  YML FROM NACOS [host:port:group:data-id];
            LOAD  JSON CONFIG FROM NACOS [host:port:group:data-id];
            LOAD  PROPERTIES FROM URL [url];
            LOAD  YAML FROM URL [url];
            LOAD  YML FROM URL [url];
            LOAD  JSON CONFIG FROM URL [url];
            
            FILE  [filePath];
            FILE  DELETE [filePath];
            FILE  RENAME [filePath]  TO [newPath];
            FILE  MOVE [filePath]  TO [newPath]  REPLACE EXISTING;
            FILE  COPY [filePath]  TO [newPath]  REPLACE EXISTING;
            FILE  MAKE [filePath];
            FILE  LENGTH [filePath];
            FILE  SIZE [filePath];
            FILE  LIST [path];
            FILE  WRITE [filePath]  APPEND [content];
            FILE  READ [filePath];
            FILE  DOWNLOAD [filePath];
            FILE  VOYAGE [filePath]  WITH {"key": "value"}  TO [newPath];

            DIR  [path];
            DIR  LIST [path];
            DIR  DELETE [path];
            DIR  RENAME [filePath]  TO [newPath];
            DIR  MOVE [path]  TO [path]  REPLACE EXISTING;
            DIR  COPY [path]  TO [path]  REPLACE EXISTING;
            DIR  MAKE [path];
            DIR  LENGTH [path];
            DIR  SPACE [path];
            DIR  SIZE [path];
            DIR  CAPACITY [path];
        """

//    --UPDATE  SHEET [sheetName]  SET &field1=value1,field2=value2,...&  WHERE |conditions|;
//    --DELETE  FROM SHEET [sheetName]  WHERE |conditions|;
//      标准的增删改查语句以后放到FQL中


    sentences.split(";")
        .foreach(sentence => {
            val sections = sentence.trim().split("""\s\s+""")  //两个空白字符以上
            if (sections.nonEmpty) {
                val caption = sections.head
                if (!Tree.contains(caption)) {
                    Tree += caption -> new mutable.LinkedHashMap[String, Int]()
                }

                if (sections.length > 1) {
                    val groups = analysePhrase(sections(1))
                    groups.foreach(group => {
                        //# 代表group
                        Tree(caption) += ("#" + HASH + group._1) -> group._2
                    })

                    for (i <- 2 until sections.length) {
                        groups.foreach(group => {
                            analysePhrase(sections(i)).foreach(phrase => {
                                Tree(caption) += (group._1 + HASH + phrase._1) -> phrase._2
                            })
                        })
                    }
                }
            }
        })

    private def analysePhrase(phrase: String): List[(String, Int)] = {

        val (mark: String, arg: Int) = phrase.takeRight(1) match {
            case "]" => ("[", Args.One)
            case ")" => ("(", if (phrase.contains("), (")) Args.More else Args.Multi)
            case "}" => ("{", Args.Map)
            case "'" => ("'", Args.Char)
            case "\"" => ("\"", Args.Char)
            case "|" => ("|", Args.Condition)
            case "*" => ("""(\S+)?\*|\([^)]+\)\*""", Args.Select)
            case "." => ("(?i)[a-z0-9]+=", Args.Set)
            case "n" => ("m", Args.Limit)
            case _ => ("", Args.None)
        }

        val name = {
            if (arg != Args.None) {
                if (arg == Args.Select || arg == Args.Set) {
                    phrase.takeBeforeX(mark.r).trim()
                }
                else {
                    phrase.takeBefore(mark).trim()
                }
            } else {
                phrase.trim()
            }
        }
        if (!name.contains("?")) {
            List[(String, Int)]((name, arg))
        }
        else {
            List[(String, Int)]((name.takeAfter("?").trim(), arg), (name.replace("?", ""), arg))
        }
    }

    def showSentences(caption: String): Unit = {
        val word = caption.toUpperCase.trim() + " "
        sentences.split(TextFile.TERMINATOR)
                 .map(_.trim())
                 .filter(_.startsWith(word))
                 .foreach(println)
    }

    def showAllSentences(): Unit = {
        sentences.split(TextFile.TERMINATOR)
                .map(_.trim())
                .foreach(println)
    }
}

case class Syntax(caption: String) {

    if (!Syntax.Tree.contains(caption)) {
        throw new SQLParseException("Wrong sentence caption: " + caption)
    }

    private val tree = Syntax.Tree(caption)

    //生成执行计划, 不考虑子语句
    //根据语句生成phrase列表，即执行计划。body语句不包括语句头关键词
    def plan(body: String): Plan = {

        val plan = new Plan()
        val chars = new mutable.ListBuffer[String]()
        //提取字符串，好处可避免参数与关键短语同名冲突
        var sentence = body.pickChars(chars)

        //去掉参数间的空格
        $ARGS.findAllIn(sentence).foreach(comma => {
            sentence = sentence.replace(comma, comma.trim())
        })

        //SELECT语句和SAVE语句的AS需要处理
        //处理 AS
        if (caption == "SELECT" || caption == "SAVE TO" || caption == "SAVE AS") {
            if (caption == "SELECT") {
                $AS.findAllIn(sentence).foreach(as => {
                    sentence = sentence.replace(as, "##AS##")
                })
            }
            else {
                """(?i)\sWITH\s+HEADERS""".r.findFirstIn(sentence) match {
                    case Some(headers) =>
                        sentence = sentence.takeBefore(headers) + headers + sentence.takeAfter(headers).replaceAll("""(?i)\s+AS\s+""", "##AS##")
                    case None =>
                }
            }
        }

        var group = "#"
        while (sentence != "") {
            val found = $PHRASE.findFirstIn(sentence).getOrElse("")
            var origin = found.trim()
            var phrase = origin.replaceAll(BLANKS, " ").toUpperCase()

            while (!tree.contains(group + Syntax.HASH + phrase) && phrase != "") {
                if (phrase.contains(" ")) {
                    phrase = phrase.takeBeforeLast(" ")
                    origin = origin.takeBeforeLastX($BLANK).trim()
                }
                else {
                    phrase = ""
                    origin = ""
                }
            }

            if (tree.contains(group + Syntax.HASH + phrase)) {
                if (origin != "") {
                    sentence = sentence.takeAfter(origin).trim()
                }
                tree(group + Syntax.HASH + phrase) match {
                    case Args.None => plan += phrase -> ""
                    case Args.One =>
                        $BLANK.findFirstIn(sentence) match {
                            case Some(blank) =>
                                plan += phrase -> sentence.takeBefore(blank).trim().restoreChars(chars)
                                sentence = sentence.takeAfter(blank).trim()
                            case None =>
                                if (sentence != "") {
                                    plan += phrase -> sentence.restoreChars(chars)
                                    sentence = ""
                                }
                                else {
                                    throw new SQLParseException("Empty arguments at phrase " + origin)
                                }
                        }
                    case Args.Multi =>
                        if (sentence.contains("(") && sentence.contains(")")) {
                            plan += phrase ->
                                        sentence.takeBefore(")")
                                            .drop(1)
                                            .split(",")
                                            .map(arg => arg.trim().restoreChars(chars))
                                            .mkString(Plan.joint)
                            sentence = sentence.takeAfter(")").trim()
                        }
                        else {
                            throw new SQLParseException("Wrong arguments format, round brackets is required: " + sentence)
                        }
                    case Args.More =>
                        if (sentence.startsWith("(") && sentence.endsWith(")")) {
                            plan += phrase ->
                                        sentence.$trim("(", ")")
                                                .split("""\),\s*\(""")
                                                .map(vs => {
                                                    vs.split(",")
                                                        .map(value => value.trim().restoreChars(chars))
                                                        .mkString(Plan.joint)
                                                })
                                                .mkString(Plan.brake)
                            sentence = ""
                        }
                        else {
                            throw new SQLParseException("Wrong arguments format, round brackets is required: " + sentence)
                        }
                    case Args.Char =>
                        if (sentence.startsWith("~str[")) {
                            plan += phrase -> chars(sentence.takeAfter("[").takeBefore("]").toInt)
                            sentence = sentence.takeAfter("]").trim()
                        }
                        else {
                            throw new SQLParseException("Wrong arguments format, must be Char: " + sentence)
                        }
                    case Args.Map =>
                        if (sentence.startsWith("{")) {
                            val brace = sentence.indexPairOf('{', '}')._2
                            plan += phrase -> sentence.substring(0, brace + 1).restoreChars(chars)
                            sentence = sentence.substring(brace + 1).trim()
                        }
                        else {
                            $BLANK.findFirstIn(sentence) match {
                                case Some(blank) =>
                                    plan += phrase -> sentence.takeBefore(blank).trim().restoreChars(chars)
                                    sentence = sentence.takeAfter(blank).trim()
                                case None =>
                                    if (sentence != "") {
                                        plan += phrase -> sentence.restoreChars(chars)
                                        sentence = ""
                                    }
                                    else {
                                        throw new SQLParseException("Empty arguments at phrase " + origin)
                                    }
                            }
                        }
                    case Args.Select =>
                        var args = ""
                        $BLANK.findFirstIn(sentence) match {
                            case Some(blank) =>
                                args = sentence.takeBefore(blank).trim()
                                sentence = sentence.takeAfter(blank).trim()
                            case None =>
                                if (sentence != "") {
                                    args = sentence
                                    sentence = ""
                                }
                                else {
                                    args = "*"
                                }
                        }
                        plan += phrase -> args.replace("##AS##", " AS ").split(",")
                                                .map(arg => arg.trim().restoreChars(chars))
                                                .mkString(Plan.joint)
                    case Args.Set =>
                        var args = ""
                        $BLANK.findFirstIn(sentence) match {
                            case Some(blank) =>
                                args = sentence.takeBefore(blank).trim()
                                sentence = sentence.takeAfter(blank).trim()
                            case None =>
                                if (sentence != "") {
                                    args = sentence
                                    sentence = ""
                                }
                                else {
                                    throw new SQLParseException("Empty arguments at phrase " + origin)
                                }
                        }
                        plan += phrase -> args.split(",")
                            .map(arg => arg.trim().restoreChars(chars))
                            .mkString(Plan.joint)

                    case Args.Condition => //只存在于WHERE/ON/HAVING/WHEN后面, 单独解析SELECT语句
                        val next = phrase match {
                            case "WHERE" =>
                                """(?i)\s(GROUP\s+BY|ORDER\s+BY|LIMIT)\s"""
                            case "ON" =>
                                """(?i)\s(INNER JOIN|LEFT JOIN|OUTER JOIN|RIGHT JOIN|WHERE)\s"""
                            case "HAVING" =>
                                """(?i)\sLIMIT\s"""
                            case "WHEN" =>
                                """(?i)\sTHEN\s"""
                            case _ => ""
                        }

                        if (next != "") {
                            next.r.findFirstIn(sentence) match {
                                case Some(word) =>
                                    val condition = sentence.takeBefore(word)
                                    plan += phrase -> condition.trim().restoreChars(chars)
                                    sentence = sentence.takeAfter(condition).trim()
                                case None =>
                                    plan += phrase -> sentence.restoreChars(chars)
                                    sentence = ""
                            }
                        }
                    case Args.Limit =>
                        plan += phrase -> sentence.restoreChars(chars)
                        sentence = ""
                    case _ =>
                }
            }
            else {
                throw new SQLParseException("Wrong phrase: " + found)
            }

            if (group == "#") {
                group = phrase
            }
        }

        chars.clear()

        plan
    }
}