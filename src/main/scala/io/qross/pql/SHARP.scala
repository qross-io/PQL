package io.qross.pql

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.ext.NumberExt._
import io.qross.ext.TypeExt._
import io.qross.fql.Fragment
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.time.TimeSpan._

import scala.collection.JavaConverters._
import scala.collection.mutable

object SHARP {

    val MULTI$ARGS$LINKS: Map[String, Set[String]] = Map[String, Set[String]](
        "SUBSTRING" -> Set[String]("TO"),
                "TAKE$BETWEEN" -> Set[String]("AND"),
                "PLUS" -> Set[String]("DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MILLISECONDS"),
                "MINUS" -> Set[String]("DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MILLISECONDS"))

    /*
    val reserved: Set[String] = Set[String]("SET", "GET", "FORMAT", "TAKE", "YEAR", "MONTH", "DAY", "WEEK", "WEEKNAME", "NANO", "HOUR", "MINUTE", "SECOND", "NUMBER")
    */

    /* ---------- 日期时间 DataTime ----------- */

    def EXPRESS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.express(arg.asText), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET/EXPRESS. " + origin)
        }
    }

    def SET(data: DataCell, arg: DataCell, origin: String): DataCell = {
        EXPRESS(data, arg, origin)
    }

    def FORMAT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.format(arg.asText), DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at FORMAT. " + origin)
        }
    }

    def SET$YEAR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setYear(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET YEAR. " + origin)
        }
    }

    def SET$MONTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setMonth(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument SET MONTH. " + origin)
        }
    }

    def SET$DAY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setDayOfMonth(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET DAY. " + origin)
        }
    }

    def SET$HOUR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setHour(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET HOUR. " + origin)
        }
    }

    def SET$MINUTE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setMinute(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET MINUTE. " + origin)
        }
    }

    def SET$SECOND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setSecond(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET SECOND. " + origin)
        }
    }

    def SET$MILLI(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setMilli(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET MILLI. " + origin)
        }
    }

    def SET$MICRO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setMicro(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET MICRO. " + origin)
        }
    }

    def SET$NANO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setNano(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET NANO. " + origin)
        }
    }

    def SET$WEEK(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setDayOfWeek(arg.asInteger.toInt), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET WEEK. " + origin)
        }
    }

    def SET$ZERO$OF$DAY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.setZeroOfDay(), DataType.DATETIME)
    }

    def SET$BEGINNING$OF$MONTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.setBeginningOfMonth(), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SET WEEK. " + origin)
        }
    }

    def GET$YEAR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getYear, DataType.INTEGER)
    }

    def GET$MONTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getMonth, DataType.INTEGER)
    }

    def GET$DAY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getDayOfMonth, DataType.INTEGER)
    }

    def GET$HOUR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getHour, DataType.INTEGER)
    }

    def GET$MINUTE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getMinute, DataType.INTEGER)
    }

    def GET$SECOND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getSecond, DataType.INTEGER)
    }

    def GET$MILLI(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getMilli, DataType.INTEGER)
    }

    def GET$MICRO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getMicro, DataType.INTEGER)
    }

    def GET$NANO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getNano, DataType.INTEGER)
    }

    def GET$WEEK(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getDayOfWeek, DataType.INTEGER)
    }

    def GET$WEEK$NAME(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asDateTime.getWeekName, DataType.TEXT)
    }

    def PLUS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS. " + origin)
        }
    }

    def PLUS$YEARS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusYears(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS YEARS. " + origin)
        }
    }

    def PLUS$MONTHS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusMonths(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS MONTHS. " + origin)
        }
    }

    def PLUS$DAYS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusDays(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS DAYS. " + origin)
        }
    }

    def PLUS$HOURS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusHours(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS HOURS. " + origin)
        }
    }

    def PLUS$MINUTES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusMinutes(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS MINUTES. " + origin)
        }
    }

    def PLUS$SECONDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusSeconds(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS SECONDS. " + origin)
        }
    }

    def PLUS$MILLIS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS MILLIS. " + origin)
        }
    }

    def PLUS$MICROS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusMicros(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS MICROS. " + origin)
        }
    }

    def PLUS$NANOS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.plusNanos(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at PLUS NANOS. " + origin)
        }
    }

    def MINUS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS. " + origin)
        }
    }

    def MINUS$YEARS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusYears(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS YEARS. " + origin)
        }
    }

    def MINUS$MONTHS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMonths(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS MONTHS. " + origin)
        }
    }

    def MINUS$DAYS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusDays(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS DAYS. " + origin)
        }
    }

    def MINUS$HOURS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusHours(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS HOURS. " + origin)
        }
    }

    def MINUS$MINUTES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMinutes(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS MINUTES. " + origin)
        }
    }

    def MINUS$SECONDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.minusSeconds(arg.asInteger).toDataCell(DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS SECONDS. " + origin)
        }
    }

    def MINUS$MILLIS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS MILLIS. " + origin)
        }
    }

    def MINUS$MICROS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS MICROS. " + origin)
        }
    }

    def MINUS$NANOS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.minusMillis(arg.asInteger), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MINUS NANOS. " + origin)
        }
    }

    def LATER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.later(arg.asDateTime).toDataCell(DataType.INTEGER)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at LATER." + origin)
        }
    }

    def EARLIER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.earlier(arg.asDateTime).toDataCell(DataType.INTEGER)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at EARLIER." + origin)
        }
    }

    def SPAN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.span(arg.asDateTime).toDataCell(DataType.INTEGER)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SPAN." + origin)
        }
    }

    def TO$EPOCH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDateTime.toEpochSecond.toDataCell(DataType.INTEGER)
    }

    def TO$EPOCH$SECOND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDateTime.toEpochSecond.toDataCell(DataType.INTEGER)
    }

    def TO$EPOCH$MILLI(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDateTime.toEpochMilli.toDataCell(DataType.INTEGER)
    }

    def TO$DATETIME(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime(arg.asText).toDataCell(DataType.DATETIME)
        }
        else {
            data.asDateTime.toDataCell(DataType.DATETIME)
        }
    }

    def MATCHES$CRON(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.matches(arg.asText).toDataCell(DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MATCHES CRON. Must specify a cron expression." + origin)
        }
    }

    /* ---------- 整数 ---------- */

    // 1 to 10
    def TO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val start = data.asInteger
            val end = arg.asInteger
            if (start <= end) {
                start.to(end).toList.asJava.toDataCell(DataType.ARRAY)
            }
            else {
                end.to(start).reverse.toList.asJava.toDataCell(DataType.ARRAY)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TO. ")
        }
    }

    // 1 until 10
    def UNTIL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val start = data.asInteger
            val end = arg.asInteger
            if (start <= end) {
                start.until(end).toList.asJava.toDataCell(DataType.ARRAY)
            }
            else {
                (end + 1).to(start).reverse.toList.asJava.toDataCell(DataType.ARRAY)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at UNTIL. ")
        }
    }

    //Timer
    def MILLI(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toDataCell(DataType.INTEGER)
    }

    def MILLIS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toDataCell(DataType.INTEGER)
    }

    def MILLISECONDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toDataCell(DataType.INTEGER)
    }

    def SECOND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.seconds.toDataCell(DataType.INTEGER)
    }

    def SECONDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.seconds.toDataCell(DataType.INTEGER)
    }

    def MINUTE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.minutes.toDataCell(DataType.INTEGER)
    }

    def MINUTES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.minutes.toDataCell(DataType.INTEGER)
    }

    def HOUR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.hours.toDataCell(DataType.INTEGER)
    }

    def HOURS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.hours.toDataCell(DataType.INTEGER)
    }

    def DAY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.days.toDataCell(DataType.INTEGER)
    }

    def DAYS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.days.toDataCell(DataType.INTEGER)
    }

    // TimeSpan
    def TO$SECONDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toSeconds.toDataCell(DataType.DECIMAL)
    }

    def TO$MINUTES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toMinutes.toDataCell(DataType.DECIMAL)
    }

    def TO$HOURS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toHours.toDataCell(DataType.DECIMAL)
    }

    def TO$DAYS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toDays.toDataCell(DataType.DECIMAL)
    }

    /* 小数 */

    def FLOOR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.floor(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.floor(0).toDataCell(DataType.DECIMAL)
        }
    }

    def ROUND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.round(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.round(0).toDataCell(DataType.DECIMAL)
        }
    }

    def POW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.pow(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.pow(0).toDataCell(DataType.DECIMAL)
        }
    }

    def PERCENT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.percent.toDataCell(DataType.TEXT)
    }

    /* ---------- 字符串处理 ---------- */

    def SPLIT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.split(arg.asText).toList.asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SPLIT. " + origin)
        }
    }

    def TAKE$LEFT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.take(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE LEFT. " + origin)
        }
    }

    def TAKE$RIGHT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.takeRight(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE RIGHT. " + origin)
        }
    }

    def TAKE$BEFORE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.takeBefore(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE BEFORE. " + origin)
        }
    }

    def TAKE$AFTER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.takeAfter(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE AFTER. " + origin)
        }
    }

    def SUBSTRING(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                val list = arg.asList[Int]
                data.asText.substring(list.head - 1, list.last - 1).toDataCell(DataType.TEXT)
            }
            else {
                data.asText.substring(arg.asInteger.toInt - 1).toDataCell(DataType.TEXT)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SUBSTRING. " + origin)
        }
    }

    /* ---------- DataTable ---------- */

    def GET$FIRST$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val default =   if (arg.valid) {
                            arg.asRow
                        }
                        else {
                            new DataRow()
                        }

        data.asTable.firstRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (arg.valid) DataCell(default, DataType.ROW) else throw new SharpLinkArgumentException("No result at GET FIRST ROW. ")
        }
    }

    def GET$LAST$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val default =   if (arg.valid) {
                            arg.asRow
                        }
                        else {
                            new DataRow()
                        }

        data.asTable.lastRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (arg.valid) DataCell(default, DataType.ROW) else throw new SharpLinkArgumentException("No result at GET FIRST ROW. ")
        }
    }

    def GET$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.getRow(arg.asInteger.toInt - 1).getOrElse(new DataRow()).toDataCell(DataType.ROW)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET ROW. " + origin)
        }
    }

    def GET$FIRST$COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.firstColumn match {
            case Some(list) => list.asJava.toDataCell(DataType.ARRAY)
            case None => throw new SharpLinkArgumentException(s"No result at GET FIRST COLUMN. ")
        }
    }

    def GET$LAST$COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.lastColumn match {
            case Some(list) => list.asJava.toDataCell(DataType.ARRAY)
            case None => throw new SharpLinkArgumentException(s"No result at GET LAST COLUMN. ")
        }
    }

    def GET$COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val field = arg.asText
            val table = data.asTable
            if (table.contains(field)) {
                table.getColumn(field).asJava.toDataCell(DataType.ARRAY)
            }
            else {
                throw new SharpLinkArgumentException(s"No result at GET COLUMN: incorrect field name $field. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET COLUMN. " + origin)
        }
    }

    def GET$FIRST$CELL$DATA(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.firstRow match {
            case Some(row) => row.firstCell
            case None => DataCell.NOT_FOUND
        }
    }

    def GET$LAST$CELL$DATA(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.lastRow match {
            case Some(row) => row.lastCell
            case None => DataCell.NOT_FOUND
        }
    }

    def GET$FIRST$$ROW$CELL$DATA(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.firstRow match {
                case Some(row) =>
                    if (arg.isInteger) {
                        row.getCell(arg.asInteger(1).toInt - 1)
                    }
                    else {
                        row.getCell(arg.asText)
                    }
                case None => DataCell.NOT_FOUND
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET FIRST ROW CELL DATA. " + origin)
        }
    }

    def GET$LAST$$ROW$CELL$DATA(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.lastRow match {
                case Some(row) =>
                    if (arg.isInteger) {
                        row.getCell(arg.asInteger(1).toInt - 1)
                    }
                    else {
                        row.getCell(arg.asText)
                    }
                case None => DataCell.NOT_FOUND
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET LAST ROW CELL DATA. " + origin)
        }
    }

    def INSERT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val table = data.asTable
            if (arg.isTable) {
                table.merge(arg.asTable).toDataCell(DataType.TABLE)
            }
            else if (arg.isRow) {
                table.addRow(arg.asRow).toDataCell(DataType.TABLE)
            }
            else if (arg.isText && Fragment.$VALUES.test(arg.asText)) {
                table.insert(arg.asText).toDataCell(DataType.TABLE)
            }
            else {
                throw new SharpLinkArgumentException(s"Incorrect arguments format at INSERT. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at INSERT. " + origin)
        }
    }

    def INSERT$IF$EMPTY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val table = data.asTable
            if (table.isEmpty) {
                INSERT(data, arg, origin)
            }
            else {
                data
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at INSERT IF EMPTY. " + origin)
        }
    }

   /* ---------- DataRow ---------- */

    def GET$DATA(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isInteger) {
                data.asRow.getCell(arg.asInteger.toInt - 1)
            }
            else {
                data.asRow.getCell(arg.asText)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET DATA. " + origin)
        }
    }

    /* ---------- DataList ---------- */

    def JOIN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val sep = if (arg.valid) {
                            arg.asText
                        }
                        else {
                            ""
                        }

        DataCell(data.asList.mkString(sep), DataType.TEXT)
    }

    def SIZE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asList.size, DataType.INTEGER)
    }

    def LENGTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        SIZE(data, arg, origin)
    }

    def HEAD(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asList.head)
    }

    def LAST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asList.last)
    }

    def GET(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val index = arg.asInteger.toInt
            val list = data.asList
            if (index < list.size) {
                DataCell(list(index))
            }
            else {
                throw new SharpLinkArgumentException(s"Out of index at GET, index: $index. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at GET. " + origin)
        }
    }

    def REVERSE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asList.reverse.asJava.toDataCell(DataType.ARRAY)
    }

    def DELIMIT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asList[String].delimit(arg.asText).toRow.toDataCell(DataType.ROW)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at DELIMIT. " + origin)
        }
    }

    /* ---------- 通用类型 ---------- */

    def TO$INTEGER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asInteger(arg.asInteger), DataType.INTEGER)
        }
        else {
            DataCell(data.asInteger, DataType.INTEGER)
        }
    }

    def TO$INT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        TO$INTEGER(data, arg, origin)
    }

    //表示子项
    def %(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell.NULL
    }
}

class SHARP(private val expression: String, private var data: DataCell = DataCell.ERROR) {

    //LET @NOW EXPRESS "DAY=1#DAY-1" FORMAT "yyyyMMdd" TO DECIMAL # ROUND # POW 2

    /*
    SHARP表达式以LET开头, LET可以省略
    处理数据的方法称为“连接”
    连接由多个单词组成，某此情况下可以是特殊字符 % (属性连接符)
    连接可以没有参数或有一个参数
    没有参数的多个连接之间必须使用 # 或 -> 或 回车换行隔开
     */

    // VALUE  v > l
    // VALUE LINK  v = l
    // VALUE LINK ARG  v > l

    def execute(PQL: PQL, quote: String = "'"): DataCell = {

        val sentence =
            if (data.invalid) {
                expression.takeAfter($LET).trim()
            }
            else {
                " " + expression
            }

        val links = new mutable.ListBuffer[Link$Argument]()

        $LINK.findAllIn(sentence)
                .reduce((m, n) => {
                    links += new Link$Argument(m, sentence.takeBetween(m, n))
                    n
                })

        for (i <- links.indices) {
            if (SHARP_LINKS.contains(links(i).linkName)) {
                data =  Class.forName("io.qross.pql.SHARP").getDeclaredMethod(links(i).linkName,
                            Class.forName("io.qross.core.DataCell"),
                            Class.forName("io.qross.core.DataCell"),
                            Class.forName("java.lang.String"))
                                .invoke(null,
                                    data,
                                    if (i + 1 < links.length) {
                                        if (SHARP.MULTI$ARGS$LINKS.contains(links(i).linkName) && i + 1 < links.length && SHARP.MULTI$ARGS$LINKS(links(i).linkName).contains(links(i+1).linkName)) {
                                            val name = links(i+1).linkName
                                            links(i+1).linkName = "" //executed
                                            Class.forName("io.qross.pql.SHARP").getDeclaredMethod(name,
                                                Class.forName("io.qross.core.DataCell"),
                                                Class.forName("io.qross.pql.Link$Argument"))
                                                    .invoke(null,
                                                        links(i).solve(PQL),
                                                        links(i+1).solve(PQL),
                                                        links(i+1).originate(PQL)
                                                    )
                                        }
                                        else {
                                            links(i).solve(PQL)
                                        }
                                    }
                                    else {
                                        DataCell.NULL
                                    },
                                    links(i).originate(PQL)
                                ).asInstanceOf[DataCell]
            }
            else {
                throw new SharpLinkArgumentException("Wrong link name: " + links(i).originalLinkName)
            }
        }

        /*

        val values = sentence.split($LINK.regex, -1).map(_.trim()).map(v => if (v == "#" || v == "->") "" else v)
        val links = $LINK.findAllIn(sentence).map(l => l.trim().replaceAll(BLANKS, "\\$").toUpperCase()).toArray

        if (data.invalid) {
            data = values.head.$sharp(PQL, quote)
        }

        for (i <- links.indices) {
            if (SHARP_LINKS.contains(links(i))) {
                data =
                    Class.forName("io.qross.pql.SHARP").getDeclaredMethod(links(i),
                    Class.forName("io.qross.core.DataCell"),
                    Class.forName("io.qross.core.DataCell"))
                        .invoke(null, data,
                                if (i + 1 < values.length) {
                                    if (SHARP.MULTI$ARGS$LINKS.contains(links(i)) && i + 1 < links.length && SHARP.MULTI$ARGS$LINKS(links(i)).contains(links(i+1))) {
                                        val name = links(i+1)
                                        links(i+1) = "" //executed
                                        Class.forName("io.qross.pql.SHARP").getDeclaredMethod(name,
                                            Class.forName("io.qross.core.DataCell"),
                                            Class.forName("io.qross.core.DataCell"))
                                                .invoke(null,
                                                    values(i+1).$sharp(PQL),
                                                    if (i + 2 < values.length) values(i+2).$sharp(PQL) else DataCell.NULL)
                                    }
                                    else if(values(i+1) != "") {
                                        values(i+1).$sharp(PQL)
                                    }
                                    else {
                                        DataCell.NULL
                                    }
                                }
                                else {
                                    DataCell.NULL
                                }
                            ).asInstanceOf[DataCell]
            }
            else {
                throw new SharpLinkArgumentException("Wrong link name: " + links(i).replace("$", " "))
            }
        }
        */

        data
    }
}

class Link$Argument(val originalLinkName: String, val originalArgument: String) {
    var linkName: String = originalLinkName.trim().replaceAll(BLANKS, "\\$").toUpperCase()
    val argument: String = {
        originalArgument.trim() match {
            case "->" | "#" => ""
            case o => o
        }
    }

    def solve(PQL: PQL): DataCell = {
        if (argument != "") {
            argument.$sharp(PQL)
        }
        else {
            DataCell.NULL
        }
    }

    def originate(PQL: PQL): String = {
        if (argument != null) {
            originalLinkName + " " + originalArgument.popStash(PQL)
        }
        else {
            originalLinkName
        }
    }
}
