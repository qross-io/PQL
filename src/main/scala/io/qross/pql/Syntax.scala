package io.qross.pql

import scala.collection.mutable

object Syntax {


    val Tree: mutable.HashMap[String, mutable.HashMap[String, mutable.LinkedHashSet[Phrase]]] = new mutable.HashMap[String, mutable.HashMap[String, mutable.LinkedHashSet[Phrase]]]()

    val sentences: String = """
            OPEN  +;
            OPEN  QROSS;
            OPEN  DEFAULT;
            OPEN  EXCEL +;

            SAVE  AS +;
            SAVE  AS DEFAULT;
            SAVE  AS QROSS;
            SAVE  AS CACHE;
            SAVE  AS CACHE TABLE +  PRIMARY KEY +;
            SAVE  AS TEMP;
            SAVE  AS TEMP TABLE +  PRIMARY KEY +  UNIQUE KEY (+)  KEY (+);
            SAVE  AS CSV FILE +  WITH HEADER *;
            SAVE  AS NEW CSV FILE +  WITH HEADER *;
            SAVE  AS TXT FILE +  WITH HEADER *  DELIMITED BY +;
            SAVE  AS NEW TXT FILE +  WITH HEADER *  DELIMITED BY +;
            SAVE  AS TEXT FILE +  WITH HEADER *  DELIMITED BY +;
            SAVE  AS NEW TEXT FILE +  WITH HEADER *  DELIMITED BY +;
            SAVE  AS EXCEL +;
            SAVE  AS NEW EXCEL +;

            INSERT  INTO SHEET +  ROW +  (+)  HEADER (+)  VALUES (+);
        """

    sentences.split(";")
        .foreach(sentence => {
            val sections = sentence.trim().split("  ")
            val caption = sections.head
            if (!Tree.contains(caption)) {
                Tree += caption -> new mutable.HashMap[String, mutable.LinkedHashSet[Phrase]]()
            }

            val initial = analyseArgs(sections(1))
            val group = sections(1).dropRight(initial._1)

            if (!Tree(caption).contains(group)) {
                Tree(caption) += group -> new mutable.LinkedHashSet[Phrase]()
                Tree(caption)(group) += new Phrase(group, initial._2)
            }

            for (i <- 2 until sections.length) {
                val args = analyseArgs(sections(i))
                Tree(caption)(group) += new Phrase(sections(i).dropRight(args._1), args._2)
            }
        })

    def analyseArgs(phrase: String): (Int, Int) = {
        phrase.takeRight(1) match {
            case "+" => (2, Args.One)
            case "*" => (2, Args.Option)
            case ")" => (4, Args.Multi)
            case _ => (0, Args.None)
        }
    }

    def showSentences(caption: String): Unit = {
        val word = caption.toUpperCase.trim() + "  "
        sentences.replace("(+)", "(value1 value2, ...)")
                 .replace("+", "arg")
                 .replace("*", "[arg]")
                 .split("\r")
                 .map(_.trim())
                 .filter(_.startsWith(word))
                 .foreach(println)
    }

    def showAllSentence(caption: String): Unit = {
        sentences.replace("(+)", "(value1 value2, ...)")
                .replace("+", "arg")
                .replace("*", "[arg]")
                .split("\r")
                .map(_.trim())
                .foreach(println)
    }

    //生成执行计划
}

object Args {
    val None = 0
    val One = 1
    val Option = 2
    val Multi = 3
}

class Phrase(words: String, args: Int = Args.One) { }