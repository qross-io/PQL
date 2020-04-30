package io.qross.pql

import scala.collection.mutable
import io.qross.pql.Patterns.{$PHRASE, $BLANK, BLANKS}
import io.qross.ext.TypeExt._

object Syntax {

    val HASH = " # "

    //caption, group # phrase, args
    val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, Int]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()
    //caption, group, phrase, arg
    //val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Int]]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Int]]]()

    val sentences: String = """
            OPEN  [JDBC-DataSource];
            OPEN  QROSS;
            OPEN  DEFAULT;
            OPEN  EXCEL [fileName];

            SAVE AS  [JDBC-DataSource]  USE [databaseName];
            SAVE AS  DEFAULT;
            SAVE AS  QROSS;
            SAVE AS  CACHE;
            SAVE AS  CACHE TABLE [tableName]  PRIMARY KEY [columnName];
            SAVE AS  TEMP;
            SAVE AS  TEMP TABLE [tableName]  PRIMARY KEY [columnName]  UNIQUE KEY (column1, column2, ...)  KEY (column1, column2, ...);
            SAVE AS  NEW? CSV FILE [fileName]  WITHOUT HEADERS;
            SAVE AS  NEW? CSV FILE [fileName]  WITH HEADERS (column1 AS header1, column2 AS header2, ...);
            SAVE AS  NEW? TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS;
            SAVE AS  NEW? TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITH HEADERS (header1, column2 AS header2, ...);
            SAVE AS  NEW? JSON FILE [fileName];
            SAVE AS  NEW? EXCEL [fileName]  USE? TEMPLATE [templateName];
            SAVE AS  EXCEL STREAM FILE [fileName]  USE? TEMPLATE [templateName];

            BLOCK  FROM [startId]  TO [endId]  PER [blockSize];

            INSERT INTO  SHEET [sheetName]  ROW [startRow]  (A, B, C, ...)  VALUES (value1, value2, ...), (value1, value2, ...);

            REQUEST  JSON API [url]  USE? METHOD [GET/POST/PUT/DELETE]  SEND? DATA {"data": "value"}  SET? HEADER {"name": "value"};

            PARSE  [path]  AS TABLE;
            PARSE  [path]  AS ROW;
            PARSE  [path]  AS MAP;
            PARSE  [path]  AS OBJECT;
            PARSE  [path]  AS LIST;
            PARSE  [path]  AS ARRAY;
            PARSE  [path]  AS VALUE;
            PARSE  [path]  AS SINGLE VALUE;
        """


//      SAVE AS  TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...);
//      SAVE AS  JSON FILE [fileName];
//      SAVE AS  EXCEL [fileName]  USE DEFAULT TEMPLATE  USE TEMPLATE [templateName];

//    --UPDATE  SHEET [sheetName]  SET &field1=value1,field2=value2,...&  WHERE |conditions|;
//    --DELETE  FROM SHEET [sheetName]  WHERE |conditions|;
//    --SELECT  *  FROM JSON FILE [fileName]  WHERE |conditions|  LIMIT m,n;


    sentences.split(";")
        .foreach(sentence => {
            val sections = sentence.trim().split("  ")
            if (sections.nonEmpty) {
                val caption = sections.head
                if (!Tree.contains(caption)) {
                    Tree += caption -> new mutable.LinkedHashMap[String, Int]()
                    //Tree += caption -> new mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Int]]()
                }

                if (sections.length > 1) {
                    val groups = analysePhrase(sections(1))
                    groups.foreach(group => {
                        //# 代表group
                        Tree(caption) += ("#" + HASH + group._1) -> group._2
                        //Tree(caption) += group._1 -> new mutable.LinkedHashMap[String, Int]()
                        //Tree(caption)(group._1) += "#" -> group._2
                    })

                    for (i <- 2 until sections.length) {
                        groups.foreach(group => {
                            analysePhrase(sections(i)).foreach(phrase => {
                                Tree(caption) += (group._1 + HASH + phrase._1) -> phrase._2
                                //Tree(caption)(group._1) += phrase._1 -> phrase._2
                            })
                        })
                    }
                }
            }
        })

    private def analysePhrase(phrase: String): List[(String, Int)] = {

        val (mark, arg) = phrase.takeRight(1) match {
            case "]" => ("[", Args.One)
            case ")" => ("(", if (phrase.contains("), (")) Args.More else Args.Multi)
            case "}" => ("{", Args.Map)
            case "'" => ("'", Args.Char)
            case "\"" => ("\"", Args.Char)
            case "|" => ("|", Args.Condition)
            case "*" => ("*", Args.Select)
            case "&" => ("&", Args.Set)
            case "n" => ("m", Args.Limit)
            case _ => ("", Args.None)
        }

        val name = if (arg != Args.None) phrase.takeBefore(mark).trim() else phrase.trim()
        if (!name.contains("?")) {
            List[(String, Int)]((name, arg))
        }
        else {
            List[(String, Int)]((name.takeAfter("?").trim(), arg), (name.replace("?", ""), arg))
        }
    }

    def showSentences(caption: String): Unit = {
        val word = caption.toUpperCase.trim() + " "
        sentences.split("\r")
                 .map(_.trim())
                 .filter(_.startsWith(word))
                 .foreach(println)
    }

    def showAllSentences(): Unit = {
        sentences.split("\r")
                .map(_.trim())
                .foreach(println)
    }
}

case class Syntax(caption: String) {

    if (!Syntax.Tree.contains(caption)) {
        throw new SQLParseException("Wrong sentence caption: " + caption)
    }

    val tree = Syntax.Tree(caption)

    //生成执行计划, 不考虑子语句
    //根据语句生成phrase列表，即执行计划。body语句不包括语句头关键词
    def plan(body: String): Plan = {

        val plan = new Plan()
        val chars = new mutable.ListBuffer[String]()
        //提取字符串，好处可避免参数与关键短语同名冲突
        var sentence = body.pickChars(chars)

//        val found = $GROUP.findFirstIn(sentence).getOrElse("")
//        var origin = found.trim()
//        var group = origin.replaceAll(BLANKS, " ").toUpperCase()
//
//        while (!tree.contains(group) && group.contains(" ")) {
//            group = group.takeBeforeLast(" ")
//            origin = origin.takeBeforeLast(" ").trim()
//        }
//
//        if (tree.contains(group)) {
//            sentence = sentence.takeAfter(origin).trim()
//
//
//        }

        var group = "#"
        while (sentence != "") {
            val found = $PHRASE.findFirstIn(sentence).getOrElse("")
            var origin = found.trim()
            var phrase = origin.replaceAll(BLANKS, " ").toUpperCase()

            while (!tree.contains(group + Syntax.HASH + phrase) && phrase.contains(" ")) {
                phrase = phrase.takeBeforeLast(" ")
                origin = origin.takeBeforeLast($BLANK).trim()
            }

            if (tree.contains(group + Syntax.HASH + phrase)) {
                sentence = sentence.takeAfter(origin).trim()
                tree(group + Syntax.HASH + phrase) match {
                    case Args.None => plan += phrase -> ""
                    case Args.One =>
                        var args = ""
                        $BLANK.findFirstIn(sentence) match {
                            case Some(blank) =>
                                args = sentence.takeBefore(blank).trim()
                                sentence = sentence.takeAfter(blank).trim()
                            case None =>
                                args = sentence.trim()
                                sentence = ""
                        }
                        if (args == "") {
                            throw new SQLParseException("Empty arguments at phrase " + origin)
                        }
                        plan += phrase -> args.restoreChars(chars)
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
                            plan += phrase -> sentence.substring(0, sentence.indexOf("}") + 1).restoreChars(chars)
                            sentence = sentence.takeAfter("}").trim()
                        }
                        else {
                            var args = ""
                            $BLANK.findFirstIn(sentence) match {
                                case Some(blank) =>
                                    args = sentence.takeBefore(blank).trim()
                                    sentence = sentence.takeAfter(blank)
                                case None =>
                                    args = sentence.trim()
                                    sentence = ""
                            }
                            if (args == "") {
                                throw new SQLParseException("Empty arguments at phrase " + origin)
                            }
                            plan += phrase -> args.restoreChars(chars)
                        }
                    case Args.Select =>
                    case Args.Set =>
                    case Args.Condition =>
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