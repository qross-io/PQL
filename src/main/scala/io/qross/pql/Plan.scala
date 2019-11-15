package io.qross.pql

import io.qross.core.{DataRow, Parameter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import io.qross.ext.TypeExt._
import io.qross.core.Parameter._

import scala.util.matching.Regex.Match

object Plan {
    val joint: String = "@,@" //戴眼镜的小胡子
    val brake: String = "<=,=>" //多行分隔使用
}

class Plan {

    def head: Phrase = phrases.head
    def last: Phrase = phrases.last

    def nonEmpty: Boolean = phrases.nonEmpty
    def isEmpty: Boolean = phrases.isEmpty
    def size: Int = phrases.size

    val phrases = new mutable.ListBuffer[Phrase]()
    val indexes = new mutable.HashMap[String, Int]()

    def +=(phrase: (String, String)): Unit = {
        indexes += phrase._1 -> phrases.size
        phrases += new Phrase(phrase._1, phrase._2)
    }

    def foreach(callback: Phrase => Unit): Unit = {
        phrases.foreach(callback)
    }

    def iterate(callback: Phrase => Unit): Unit = {
        for(i <- 1 until phrases.length) {
            callback(phrases(i))
        }
    }

    def options: ListBuffer[Phrase] = phrases.drop(1)

    def contains(words: String): Boolean = indexes.contains(words)
    def get(words: String): Phrase = phrases(indexes(words))
}

class Phrase(val words: String, var args: String) {

    lazy private val phType: Int = args.placeHolderType
    lazy private val phMatches: List[Match] = {
        if (phType == Parameter.SHARP) {
            args.pickParameters()
        }
        else {
            if (phType == Parameter.MARK) {
                args = args.pickQuestionMarks()
            }
            List[Match]()
        }
    }

    def oneArgs: String = args.removeQuotes()
    def multiArgs: Array[String] = args.split(Plan.joint).map(_.removeQuotes())
    def moreArgs: Array[Array[String]] = args.split(Plan.brake).map(_.split(Plan.joint).map(_.removeQuotes()))

    //用数据替换占位符再返回
    def moreArgs(row: DataRow): Array[Array[String]] = {
        {
            if (row.nonEmpty) {
                phType match {
                    case SHARP => args.replaceParameters(phMatches, row)
                    case MARK => args.replaceQuestionMarks(row)
                    case NONE => args
                }
            }
            else {
                args
            }
        }.split(Plan.brake).map(_.split(Plan.joint).map(_.removeQuotes()))
    }
}
