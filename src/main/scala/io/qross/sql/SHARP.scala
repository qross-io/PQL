package io.qross.sql

import io.qross.core.{DataCell, DataType}
import io.qross.ext.TypeExt._
import io.qross.sql.Patterns._
import io.qross.sql.Solver._
import io.qross.time.DateTime

import scala.collection.JavaConverters._

object SHARP {

    /*
    val reserved: Set[String] = Set[String]("SET", "GET", "FORMAT", "TAKE", "YEAR", "MONTH", "DAY", "WEEK", "WEEKNAME", "NANO", "HOUR", "MINUTE", "SECOND", "NUMBER")

    val EXECUTOR: Map[String, (Any, Any) => Any] = Map[String, (Any, Any) => Any](
        "SET" -> SET,
        "FORMAT" -> FORMAT,
        "GET$YEAR" -> GET$YEAR,
        "GET$MONTH" -> GET$MONTH,
        "GET$DAY" -> GET$DAY,
        "GET$HOUR" -> GET$HOUR,
        "GET$MINUTE" -> GET$MINUTE,
        "GET$SECOND" -> GET$SECOND,
        "GET$NANO" -> GET$NANO,
        "GET$WEEK" -> GET$WEEK,
        "GET$WEEKNAME" -> GET$WEEKNAME,
        "GET$NUMBER" -> GET$NUMBER,
    )
    */

    //时间函数
    def SET(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.express(args.head.asText), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET, arguments $args.length, data type: ${data.dataType} ")
        }
    }

    def FORMAT(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.format(args.head.asText), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at FORMAT, arguments $args.length, data type: ${data.dataType} ")
        }
    }

    def GET$YEAR(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getYear, DataType.INTEGER)
    }

    def GET$MONTH(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getMonth, DataType.INTEGER)
    }

    def GET$DAY(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getDayOfMonth, DataType.INTEGER)
    }

    def GET$HOUR(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getHour, DataType.INTEGER)
    }

    def GET$MINUTE(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getMinute, DataType.INTEGER)
    }

    def GET$SECOND(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getSecond, DataType.INTEGER)
    }

    def GET$NANO(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getNano, DataType.INTEGER)
    }

    def GET$WEEK(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getDayOfWeek, DataType.INTEGER)
    }

    def GET$WEEK$NAME(data: DataCell, args: Seq[DataCell]): DataCell = {
        DataCell(data.asDateTime.getWeekName, DataType.INTEGER)
    }

    def TO$INTEGER(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
             DataCell(data.asInteger(args.head.asInteger), DataType.INTEGER)
        }
        else {
            DataCell(data.asInteger, DataType.INTEGER)
        }
    }

    def TO$INT(data: DataCell, args: Seq[DataCell]): DataCell = {
        TO$INTEGER(data, args)
    }

    def TO(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asInteger.to(args.head.asInteger).toList.asJava, DataType.ARRAY)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TO, arguments ${args.length}, data type: ${data.dataType} ")
        }
    }

    def UNTIL(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asInteger.until(args.head.asInteger).toList.asJava, DataType.ARRAY)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at UNTIL, arguments $args.length, data type: ${data.dataType} ")
        }
    }

    def SPLIT(data: DataCell, args: Seq[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.length == 1) {
                DataCell(data.asText.split(args.head.asText).toList.asJava, DataType.ARRAY)
            }
            else {
                DataCell(data.asText.split(args.head.asText, args(1).asInteger.toInt).toList.asJava, DataType.ARRAY)
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at UNTIL, arguments $args.length, data type: ${data.dataType} ")
        }
    }
}

class SHARP(val expression: String) {

    //LET @NOW SET "DAY=1#DAY-1" GET STRING "yyyyMMdd" TO DECIMAL -> ROUND -> POW 2

    private val sentence: String = expression.takeAfter($LET).trim()
    private val values = sentence.split($LINK.regex).map(a => a.trim)
    private val links = $LINK.findAllIn(sentence).map(l => l.trim().replaceAll("""\s+""", "$").toUpperCase()).toArray

    // VALUE  v > l
    // VALUE LINK  v = l
    // VALUE LINK ARG  v > l

    def execute(PSQL: PSQL): DataCell = {

        var data = values(0).$sharp(PSQL)

        for (i <- links.indices) {
            if (SHARP_LINKS.contains(links(i))) {
                data = SHARP.getClass.getDeclaredMethod(links(i)).invoke(null, data,
                    if (i + 1 < values.length) {
                        values(i + 1).split(",").filter(arg => arg != "->").map(arg => arg.popStash(PSQL).eval()).toSeq
                    }
                    else {
                        Seq[DataCell]()
                    }).asInstanceOf[DataCell]
            }
            else {
                throw new SQLExecuteException("Wrong link name: " + links(i).replace("$", " "))
            }
        }
        data
    }
}