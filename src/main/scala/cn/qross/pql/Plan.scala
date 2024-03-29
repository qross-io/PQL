package cn.qross.pql

import cn.qross.core.{DataRow, Parameter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import cn.qross.ext.TypeExt._
import cn.qross.core.Parameter._
import cn.qross.net.Json

object Plan {
    val joint: String = "@,@" //戴眼镜的小胡子
    val brake: String = "<=,=>" //多行分隔使用
}

class Plan {

    val phrases = new mutable.LinkedHashMap[String, String]()

    def head: String = phrases.head._1
    def last: String = phrases.last._1
    def size: Int = phrases.size

    def +=(phrase: (String, String)): Unit = {
        phrases += phrase._1 -> phrase._2
    }

    //不遍历第一个
    def options: Iterable[String] = phrases.keys.drop(1)

    def contains(phrase: String, alternativePhrase: String = ""): Boolean = {
        if (!phrases.contains(phrase)) {
            if (alternativePhrase != "") {
                phrases.contains(alternativePhrase)
            }
            else {
                false
            }
        }
        else {
            true
        }
    }

    def get(phrase: String, alternativePhrase: String = ""): Option[String] = {
        if (phrases.contains(phrase)) {
            phrases.get(phrase)
        }
        else if (alternativePhrase != "") {
            phrases.get(alternativePhrase)
        }
        else {
            None
        }
    }

    def headArgs: String = phrases.head._2.removeQuotes()
    def lastArgs: String = phrases.last._2.removeQuotes()

    def oneArgs(phrase: String, alternativePhrase: String = ""): String = get(phrase, alternativePhrase).getOrElse("").removeQuotes()

    def multiArgs(phrase: String, alternativePhrase: String = ""): Array[String] = {
        get(phrase, alternativePhrase) match {
            case Some(args) => args.split(Plan.joint).map(_.removeQuotes())
            case None => Array[String]()
        }
    }

    def mapArgs(phrase: String, alternativePhrase: String = ""): Map[String, String] = {
        val args = get(phrase, alternativePhrase).getOrElse("")
        if (args.bracketsWith("{", "}")) {
            Json(args).parseMap("/")
        }
        else if (args != "") {
            args.removeQuotes().splitToMap()
        }
        else {
            Map[String, String]()
        }
    }

    def moreArgs(phrase: String, alternativePhrase: String = ""): Array[Array[String]] = {
        get(phrase, alternativePhrase) match {
            case Some(args) => args.split(Plan.brake).map(_.split(Plan.joint).map(_.removeQuotes()))
            case None => Array[Array[String]]()
        }
    }

    //用数据替换占位符再返回
    def moreArgs(row: DataRow, phrase: String): Array[Array[String]] = {
        val args = get(phrase).getOrElse("")

        {
            if (row.nonEmpty) {
                args.placeHolderType match {
                    case SHARP => args.replaceParameters(args.pickParameters(), row)
                    case MARK => args.pickQuestionMarks().replaceQuestionMarks(row)
                    case NONE => args
                }
            }
            else {
                args
            }
        }.split(Plan.brake).map(_.split(Plan.joint).map(_.removeQuotes()))
    }

    def setArgs(phrase: String, alternativePhrase: String = ""): Map[String, String] = {
        val args = get(phrase, alternativePhrase).getOrElse("")
        if (args.contains(Plan.joint)) {
            args.split(Plan.joint).filter(_.contains("=")).map(pair => {
                (pair.takeBefore("="), pair.takeAfter("=").removeQuotes())
            }).toMap
        }
        else {
            args.removeQuotes().splitToMap()
        }
    }

    //select -> list
    def listArgs(phrase: String, alternativePhrase: String = ""): Array[String] = {
        val args = get(phrase, alternativePhrase).getOrElse("")
        if (args.contains(Plan.joint)) {
            args.split(Plan.joint).map(_.removeQuotes())
        }
        else if (args.contains(",")) {
            args.removeQuotes().split(",")
        }
        else {
            Array[String](args.removeQuotes())
        }
    }

    def selectArgs(phrase: String, alternativePhrase: String = ""): Array[(String, String)] = {
        get(phrase, alternativePhrase)
            .getOrElse("")
            .$trim("(", ")")
            .split(Plan.joint)
            .map(field => {
                if (field.contains(" AS ")) {
                    (field.takeBefore(" AS "), field.takeAfter(" AS "))
                }
                else {
                    (field, field)
                }
            })
    }

    def limitArgs(phrase: String, alternativePhrase: String = ""): (String, String) = {
        val args = get(phrase, alternativePhrase).getOrElse("")

        if (args.contains(Plan.joint)) {
            (args.takeBefore(Plan.joint).removeQuotes(), args.takeAfter(Plan.joint).removeQuotes())
        }
        else {
            (args.removeQuotes(), "")
        }
    }
}