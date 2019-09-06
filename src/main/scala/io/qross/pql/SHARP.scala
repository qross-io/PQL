package io.qross.pql

import com.fasterxml.jackson.databind.JsonNode
import io.qross.core.{DataCell, DataRow, DataTable, DataType}
import io.qross.ext.TypeExt._
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
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

    /* ---------- 日期时间 DataTime ----------- */

    def EXPRESS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.express(args.head.asText), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET/EXPRESS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET(data: DataCell, args: List[DataCell]): DataCell = {
        EXPRESS(data, args)
    }

    def FORMAT(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.format(args.head.asText), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at FORMAT, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$YEAR(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setYear(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET YEAR, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$MONTH(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setMonth(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET MONTH, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$DAY(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setDayOfMonth(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET DAY, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$HOUR(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setHour(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET HOUR, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$MINUTE(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setMinute(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET MINUTE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$SECOND(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setSecond(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET SECOND, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$NANO(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setNano(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET NANO, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SET$WEEK(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.setDayOfWeek(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at SET WEEK, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def GET$YEAR(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getYear, DataType.INTEGER)
    }

    def GET$MONTH(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getMonth, DataType.INTEGER)
    }

    def GET$DAY(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getDayOfMonth, DataType.INTEGER)
    }

    def GET$HOUR(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getHour, DataType.INTEGER)
    }

    def GET$MINUTE(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getMinute, DataType.INTEGER)
    }

    def GET$SECOND(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getSecond, DataType.INTEGER)
    }

    def GET$NANO(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getNano, DataType.INTEGER)
    }

    def GET$WEEK(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getDayOfWeek, DataType.INTEGER)
    }

    def GET$WEEK$NAME(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asDateTime.getWeekName, DataType.TEXT)
    }

    def PLUS$YEARS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusYears(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS YEARS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def PLUS$MONTHS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusMonths(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS MONTHS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def PLUS$DAYS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusDays(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS DAYS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def PLUS$HOURS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusHours(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS HOURS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def PLUS$MINUTES(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusMinutes(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS MINUTES, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def PLUS$SECONDS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.plusSeconds(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at PLUS SECONDS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$YEARS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusYears(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS YEARS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$MONTHS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusMonths(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS MONTHS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$DAYS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusDays(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS DAYS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$HOURS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusHours(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS HOURS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$MINUTES(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusMinutes(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS MINUTES, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def MINUS$SECONDS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asDateTime.minusSeconds(args.head.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at MINUS SECONDS, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    /* ---------- 整数 ---------- */

    // 1 to 10
    def TO(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asInteger.to(args.head.asInteger).toList.asJava, DataType.ARRAY)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TO, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    // 1 until 10
    def UNTIL(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asInteger.until(args.head.asInteger).toList.asJava, DataType.ARRAY)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at UNTIL, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    /* ---------- 字符串处理 ---------- */

    def SPLIT(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                DataCell(data.asText.split(args.head.asText).toList.asJava, DataType.ARRAY)
            }
            else {
                DataCell(data.asText.split(args.head.asText, args(1).asInteger.toInt).toList.asJava, DataType.ARRAY)
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at UNTIL, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def TAKE(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asText.take(args.head.asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TAKE BEFORE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def TAKE$RIGHT(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asText.takeRight(args.head.asInteger.toInt), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TAKE BEFORE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def TAKE$BEFORE(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asText.takeBefore(args.head.asText), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TAKE BEFORE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def TAKE$AFTER(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asText.takeAfter(args.head.asText), DataType.TEXT)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TAKE BEFORE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def SUBSTRING(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.size == 1) {
                DataCell(data.asText.substring(args.head.asInteger.toInt - 1), DataType.TEXT)
            }
            else {
                DataCell(data.asText.substring(args.head.asInteger.toInt - 1, args(1).asInteger.toInt - 1), DataType.TEXT)
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at TAKE BEFORE, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    /* ---------- DataTable ---------- */

    def INSERT$IF$NOT$EXISTS(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val table = data.asTable
            if (args.head.isTable) {
                DataCell(table.merge(args.head.asTable), DataType.TABLE)
            }
            else if (args.head.isRow) {
                DataCell(table.addRow(args.head.asRow), DataType.TABLE)
            }
            else {
                val names = table.getFieldNames
                val fields = table.getFields
                val row = table.newRow()
                for (i <- args.indices) {
                    if (i < fields.size) {
                        row.set(names(i), args(i).to(fields(names(i))))
                    }
                }
                table.insert(row)
                DataCell(table, DataType.TABLE)
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at INSERT IF EMPTY, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def GET$FIRST$ROW(data: DataCell, args: List[DataCell]): DataCell = {
        val default =   if (args.nonEmpty) {
                            args.head.asRow
                        }
                        else {
            new DataRow()
                        }

        data.asTable.firstRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (args.nonEmpty) DataCell(default, DataType.ROW) else throw new SQLExecuteException("No result at GET FIRST ROW")
        }
    }

    def GET$LAST$ROW(data: DataCell, args: List[DataCell]): DataCell = {
        val default =   if (args.nonEmpty) {
                            args.head.asRow
                        }
                        else {
            new DataRow()
                        }

        data.asTable.lastRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (args.nonEmpty) DataCell(default, DataType.ROW) else throw new SQLExecuteException("No result at GET FIRST ROW")
        }
    }

    def GET$ROW(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asTable.getRow(args.head.asInteger.toInt - 1).getOrElse(new DataRow()), DataType.ROW)
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at GET ROW, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def GET$FIRST$COLUMN(data: DataCell, args: List[DataCell]): DataCell = {
        data.asTable.firstColumn match {
            case Some(list) => DataCell(list, DataType.ARRAY)
            case None => throw new SQLExecuteException(s"No result at GET FIRST COLUMN")
        }
    }

    def GET$LAST$COLUMN(data: DataCell, args: List[DataCell]): DataCell = {
        data.asTable.lastColumn match {
            case Some(list) => DataCell(list, DataType.ARRAY)
            case None => throw new SQLExecuteException(s"No result at GET LAST COLUMN")
        }
    }

    def GET$COLUMN(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val field = args.head.asText
            val table = data.asTable
            if (table.contains(field)) {
                DataCell(table.getColumn(field), DataType.ARRAY)
            }
            else {
                throw new SQLExecuteException(s"No result at GET COLUMN: incorrect field name $field")
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at GET COLUMN, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    def GET$FIRST$CELL$DATA(data: DataCell, args: List[DataCell]): DataCell = {
        data.asTable.firstRow match {
            case Some(row) => row.firstCell
            case None => DataCell.NOT_FOUND
        }
    }

    def GET$LAST$CELL$DATA(data: DataCell, args: List[DataCell]): DataCell = {
        data.asTable.lastRow match {
            case Some(row) => row.lastCell
            case None => DataCell.NOT_FOUND
        }
    }

    def GET$CELL$DATA(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.size == 2) {
            if (args(1).isInteger) {
                data.asTable.getCell(args.head.asInteger(0).toInt - 1, args(1).asInteger(0).toInt - 1)
            }
            else {
                data.asTable.getCell(args.head.asInteger(0).toInt - 1, args(1).asText)
            }
        }
        else {
            throw new SQLExecuteException(s"Incorrect arguments length at GET CELL DATA, expected: 2, actual: ${args.size} ")
        }
    }

    /* ---------- DataRow ---------- */

    def GET$DATA(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            if (args.head.isInteger) {
                data.asRow.getCell(args.head.asInteger.toInt - 1)
            }
            else {
                data.asRow.getCell(args.head.asText)
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at GET DATA, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    /* ---------- DataList ---------- */

    def JOIN(data: DataCell, args: List[DataCell]): DataCell = {
        val sep = if (args.nonEmpty) {
                            args.head.asText
                        }
                        else {
                            ""
                        }

        DataCell(data.asList.mkString(sep), DataType.TEXT)
    }

    def SIZE(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asList.size, DataType.INTEGER)
    }

    def LENGTH(data: DataCell, args: List[DataCell]): DataCell = {
        SIZE(data, args)
    }

    def HEAD(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asList.head)
    }

    def LAST(data: DataCell, args: List[DataCell]): DataCell = {
        DataCell(data.asList.last)
    }

    def GET(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            val index = args.head.asInteger.toInt
            val list = data.asList
            if (index < list.size) {
                DataCell(list(index))
            }
            else {
                throw new SQLExecuteException(s"Out of index at GET, index: $index")
            }
        }
        else {
            throw new SQLExecuteException(s"Empty arguments or incorrect data type at GET, arguments ${args.size}, data type: ${data.dataType} ")
        }
    }

    /* ---------- 通用类型 ---------- */

    def TO$INTEGER(data: DataCell, args: List[DataCell]): DataCell = {
        if (args.nonEmpty) {
            DataCell(data.asInteger(args.head.asInteger), DataType.INTEGER)
        }
        else {
            DataCell(data.asInteger, DataType.INTEGER)
        }
    }

    def TO$INT(data: DataCell, args: List[DataCell]): DataCell = {
        TO$INTEGER(data, args)
    }
}

class SHARP(private val expression: String, private var data: DataCell = DataCell.NOT_FOUND) {

    //LET @NOW EXPRESS "DAY=1#DAY-1" FORMAT "yyyyMMdd" TO DECIMAL -> ROUND -> POW 2

    // VALUE  v > l
    // VALUE LINK  v = l
    // VALUE LINK ARG  v > l

    def execute(PQL: PQL): DataCell = {

        val sentence =
            if (data.invalid) {
                expression.takeAfter($LET).trim()
            }
            else {
                "EMPTY " + expression
            }

        val values = sentence.split($LINK.regex, -1).map(_.trim())
        val links = $LINK.findAllIn(sentence).map(l => l.trim().replaceAll(BLANKS, "\\$").toUpperCase()).toArray

        if (data.invalid) {
            data = values.head.$sharp(PQL)
        }

        for (i <- links.indices) {
            if (SHARP_LINKS.contains(links(i))) {
                data =
                    Class.forName("io.qross.pql.SHARP").getDeclaredMethod(links(i),
                    Class.forName("io.qross.core.DataCell"),
                    Class.forName("scala.collection.immutable.List"))
                        .invoke(null, data,
                            if (i + 1 < values.length) {
                                values(i + 1).toArgs(PQL)
                            }
                            else {
                                List[DataCell]()
                            }).asInstanceOf[DataCell]
            }
            else {
                throw new SQLExecuteException("Wrong link name: " + links(i).replace("$", " "))
            }
        }

        data
    }
}

