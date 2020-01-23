package io.qross.pql

import scala.collection.mutable
import io.qross.pql.Patterns.{$PHRASE, $BLANK, BLANKS}
import io.qross.ext.TypeExt._

object Syntax {

    val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, Int]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()

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
            SAVE AS  CSV FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 header2, ...);
            SAVE AS  NEW CSV FILE [fileName]  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...);
            SAVE AS  TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (column1 AS header1, column2 AS header2, ...);
            SAVE AS  NEW TXT FILE [fileName]  DELIMITED BY 'delimiter'  WITHOUT HEADERS  WITH HEADERS (header1, column2 AS header2, ...);
            SAVE AS  JSON FILE [fileName];
            SAVE AS  NEW JSON FILE [fileName];
            SAVE AS  EXCEL [fileName]  USE DEFAULT TEMPLATE  USE TEMPLATE [templateName];
            SAVE AS  NEW EXCEL [fileName]  USE DEFAULT TEMPLATE  USE TEMPLATE [templateName];

            SELECT  *  FROM JSON FILE [fileName]  WHERE {conditions}  LIMIT m|m,n;

            INSERT INTO  SHEET [sheetName]  ROW [startRow]  (A, B, C, ...)  VALUES (value1, value2, ...), (value1, value2, ...);
            UPDATE  SHEET [sheetName]  SET [field1=value1,field2=value2,...]  WHERE {conditions};
            DELETE  FROM SHEET [sheetName]  WHERE {conditions};

            REQUEST  JSON API [url];
        """

    sentences.split(";")
        .foreach(sentence => {
            val sections = sentence.trim().split("  ")
            if (sections.length > 1) {
                val caption = sections.head
                if (!Tree.contains(caption)) {
                    Tree += caption -> new mutable.LinkedHashMap[String, Int]()
                }

                val initial = analyseArgs(sections(1))
                val group = if (initial._2 != Args.None) sections(1).takeBefore(initial._1).trim() else sections(1).trim()

                if (!Tree(caption).contains(group)) {
                    Tree(caption) += (" # " + group) -> initial._2
                }

                for (i <- 2 until sections.length) {
                    val args = analyseArgs(sections(i))
                    val phrase = if (args._2 != Args.None) sections(i).takeBefore(args._1).trim() else sections(i).trim()
                    Tree(caption) += (group + " # " + phrase) -> args._2
                }
            }
        })

    private def analyseArgs(phrase: String): (String, Int) = {
        phrase.takeRight(1) match {
            case "]" => ("[", Args.One)
            case ")" => ("(", if (phrase.contains("), (")) Args.More else Args.Multi)
            case _ => ("", Args.None)
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

    //生成执行计划, 先不考虑子语句

    //根据语句生成phrase列表
    def plan(body: String): Plan = {

        //val phrases = new mutable.LinkedHashMap[String, Any]()
        val plan = new Plan()
        val chars = new mutable.ListBuffer[String]()
        var sentence = body.pickChars(chars)

        var group = ""
        while (sentence != "") {
            val found = $PHRASE.findFirstIn(sentence).getOrElse("")
            var origin = found.trim()
            var phrase = origin.replaceAll(BLANKS, " ").toUpperCase()

            while (!tree.contains(group + " # " + phrase) && phrase.contains(" ")) {
                phrase = phrase.takeBeforeLast(" ")
                origin = origin.takeBeforeLast($BLANK).trim()
            }

            if (tree.contains(group + " # " + phrase)) {
                sentence = sentence.takeAfter(origin).trim()
                tree(group + " # " + phrase) match {
                    case Args.One =>
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
                    case Args.Set =>
                    case Args.Condition =>
                    case Args.Char =>
                    case _ =>
                }
            }
            else {
                throw new SQLParseException("Wrong phrase: " + found)
            }

            if (group == "") {
                group = phrase
            }
        }

        chars.clear()

        plan
    }
}

//使用?表示可选，使用+表示一个或多个
object Args {
    val None = 0 //无参数
    val One = 1 //一个参数, 参数名字中无空格, 和方括号包围
    val Multi = 2 //多参, 用小括号包围
    val More = 3  //one or more, Multi的多个重复,  必须是最后一个参数
    val Select = 4 //一个参数或多个参数, 参数之间使用逗号隔开, 可包含单词，主要是SELECT语句中的select和limit, 依靠下一个phrase的名字识别
    val Set = 5 //一整套参数, 主要是update中的set, 依靠下一个phrase的名字识别
    val Condition = 6 //条件, 主要是where, 依靠下一个phrase的名字识别
    val Char = 7 //字符串
}