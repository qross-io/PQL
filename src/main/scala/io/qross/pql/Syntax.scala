package io.qross.pql

import scala.collection.mutable
import io.qross.pql.Patterns.{$CAPTION, $BLANK, BLANKS}
import io.qross.ext.TypeExt._

object Syntax {

    val Tree: mutable.HashMap[String, mutable.LinkedHashMap[String, Int]] = new mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()

    val sentences: String = """
            OPEN  [JDBC-DataSource];
            OPEN  QROSS;
            OPEN  DEFAULT;
            OPEN  EXCEL [fileName];

            SAVE  AS [JDBC-DataSource];
            SAVE  AS DEFAULT;
            SAVE  AS QROSS;
            SAVE  AS CACHE;
            SAVE  AS CACHE TABLE [tableName]  PRIMARY KEY [columnName];
            SAVE  AS TEMP;
            SAVE  AS TEMP TABLE [tableName]  PRIMARY KEY [columnName]  UNIQUE KEY (column1, column2, ...)  KEY (column1, column2, ...);
            SAVE  AS CSV FILE [fileName]  WITHOUT HEADER  WITH HEADER (column1 AS header1, column2 header2, ...);
            SAVE  AS NEW CSV FILE [fileName]  WITHOUT HEADER  WITH HEADER (column1 AS header1, column2 AS header2, ...);
            SAVE  AS TXT FILE [fileName]  WITHOUT HEADER  WITH HEADER (column1 AS header1, column2 AS header2, ...)  DELIMITED BY ['delimiter'];
            SAVE  AS NEW TXT FILE [fileName]  WITHOUT HEADER  WITH HEADER (header1, column2 AS header2, ...)  DELIMITED BY ['delimiter'];
            SAVE  AS EXCEL [fileName];
            SAVE  AS NEW EXCEL [fileName];

            INSERT  INTO SHEET [sheetName]  ROW [startRow]  (column1, column2, ...)  VALUES (value1, value2, ...);
        """

    sentences.split(";")
        .foreach(sentence => {
            val sections = sentence.trim().split("  ")
            val caption = sections.head
            if (!Tree.contains(caption)) {
                Tree += caption -> new mutable.LinkedHashMap[String, Int]()
            }

            val initial = analyseArgs(sections(1))
            val group = if (initial._2 != Args.None) sections(1).takeBeforeLast(initial._1) else sections(1)

            if (!Tree(caption).contains(group)) {
                Tree(caption) += (group + " # ") -> initial._2
            }

            for (i <- 2 until sections.length) {
                val args = analyseArgs(sections(i))
                val phrase = if (args._2 != Args.None) sections(i).takeBeforeLast(args._1) else sections(i)
                Tree(caption) += (group + " # " + phrase) -> args._2
            }
        })

    private def analyseArgs(phrase: String): (String, Int) = {
        phrase.takeRight(1) match {
            case "]" => ("[", Args.One)
            case ")" => ("(", Args.Multi)
            case _ => ("", Args.None)
        }
    }

    def showSentences(caption: String): Unit = {
        val word = caption.toUpperCase.trim() + "  "
        sentences.split("\r")
                 .map(_.trim())
                 .filter(_.startsWith(word))
                 .foreach(println)
    }

    def showAllSentence(caption: String): Unit = {
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
    def plan(body: String): mutable.ListBuffer[Phrase] = {

        val chars = new mutable.ListBuffer[String]()
        var sentence = body.pickChars(chars)

        val list = new mutable.ListBuffer[Phrase]()

        var group = ""
        while (sentence != "") {
            val found = $CAPTION.findFirstIn(sentence).getOrElse("")
            var origin = found.trim()
            var phrase = origin.replaceAll(BLANKS, " ").toUpperCase()
            var args = ""

            while (!tree.contains(phrase) && phrase.contains(" ")) {
                phrase = phrase.takeBeforeLast(" ")
                origin = origin.takeBeforeLast($BLANK).trim()
            }

            val key = group + " # " + phrase
            if (tree.contains(key)) {
                sentence = sentence.takeAfter(origin).trim()
                tree(key) match {
                    case Args.One =>
                        $BLANK.findFirstIn(sentence) match {
                            case Some(blank) =>
                                args = sentence.takeBefore(blank)
                                sentence = sentence.takeAfter(blank)
                            case None => args = sentence
                        }
                    case Args.Multi =>
                        if (sentence.contains("(") && sentence.contains(")")) {
                            args = sentence.takeBefore(")") + ")"
                        }
                        else {
                            throw new SQLParseException("Wrong arguments format: " + sentence)
                        }
                    case _ => args = ""
                }
                list += new Phrase(group, args)
            }

            if (group == "") {
                group = phrase
            }
        }

        list
    }
}

object Args {
    val None = 0
    val One = 1
    val Multi = 2
}