package io.qross.pql

import java.util

import io.qross.core.{DataCell, DataRow, DataType}
import io.qross.exception.{SharpDataExceptionException, SharpInapplicableLinkNameException, SharpLinkArgumentException}
import io.qross.ext.TypeExt._
import io.qross.fql.Fragment
import io.qross.net.Json
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.security.{Base64, MD5}
import io.qross.setting.Global
import io.qross.time.ChronExp
import io.qross.time.TimeSpan._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object Sharp {

    /* ---------- 日期时间 DataTime ----------- */

    def EXPRESS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDateTime.express(arg.asText), DataType.DATETIME)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at EXPRESS. " + origin)
        }
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
        DataCell(data.asDateTime.setBeginningOfMonth(), DataType.DATETIME)
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
            //integer
            if (data.isInteger) {
                if (arg.isInteger) {
                    DataCell(data.asInteger(0) + arg.asInteger(0), DataType.INTEGER)
                }
                else {
                    DataCell(data.asInteger(0) + arg.asDecimal(0), DataType.DECIMAL)
                }
            }
            //decimal
            else if (data.isDecimal) {
                DataCell(data.asDecimal(0) + arg.asDecimal(0), DataType.DECIMAL)
            }
            //datetime
            else {
                if (arg.isInteger) {
                    DataCell(data.asDateTime.plusMillis(arg.asInteger), DataType.DATETIME)
                }
                else {
                    $DATETIME_UNITS.findFirstMatchIn(arg.asText) match {
                        case Some(m) => data.asDateTime.plus(m.group(1), m.group(2).toInt).toDataCell(DataType.DATETIME)
                        case None => throw SharpLinkArgumentException.occur("PLUS", origin)
                    }
                }
            }
        }
        else {
            throw SharpLinkArgumentException.occur("PLUS", origin)
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
            if (data.isInteger) {
                if (arg.isInteger) {
                    DataCell(data.asInteger(0) - arg.asInteger(0), DataType.INTEGER)
                }
                else {
                    DataCell(data.asInteger(0) - arg.asDecimal(0), DataType.DECIMAL)
                }
            }
            else if (data.isDecimal) {
                DataCell(data.asDecimal(0) - arg.asDecimal(0), DataType.DECIMAL)
            }
            else {
                if (arg.isInteger) {
                    DataCell(data.asDateTime.minusMillis(arg.asInteger), DataType.DATETIME)
                }
                else {
                    $DATETIME_UNITS.findFirstMatchIn(arg.asText) match {
                        case Some(m) => data.asDateTime.minus(m.group(1), m.group(2).toInt).toDataCell(DataType.DATETIME)
                        case None => throw SharpLinkArgumentException.occur("MINUS", origin)
                    }
                }
            }
        }
        else {
            throw SharpLinkArgumentException.occur("MINUS", origin)
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

    def BEFORE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.before(arg.asDateTime).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur(s"BEFORE", origin)
        }
    }

    def AFTER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.after(arg.asDateTime).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur(s"AFTER", origin)
        }
    }

    def BEFORE$OR$EQUALS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.beforeOrEquals(arg.asDateTime).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur(s"BEFORE OR EQUALS", origin)
        }
    }


    def AFTER$OR$EQUALS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDateTime.afterOrEquals(arg.asDateTime).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur(s"AFTER OR EQUALS", origin)
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
            data.asDateTime.matches(arg.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at MATCHES CRON. Must specify a cron expression." + origin)
        }
    }

    /* ---------- 介词 ---------- */

    def AND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger && arg.isInteger) {
                List[Int](data.asInteger.toInt, arg.asInteger.toInt).asJava.toDataCell(DataType.ARRAY)
            }
            else if (data.isBoolean || arg.isBoolean) {
                DataCell(data.asBoolean && arg.asBoolean, DataType.BOOLEAN)
            }
            else {
                List[Any](data.value, arg.value).asJava.toDataCell(DataType.ARRAY)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("AND", origin)
        }
    }

    def OR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isBoolean || arg.isBoolean) {
                DataCell(data.asBoolean || arg.asBoolean, DataType.BOOLEAN)
            }
            else {
                DataCell(data.asBoolean || arg.asBoolean, DataType.BOOLEAN)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("OR", origin)
        }
    }

    def LESS$THAN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data < arg, DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("LESS THAN", origin)
        }
    }

    def LESS$THAN$OR$EQUALS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data <= arg, DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("LESS THAN OR EQUALS", origin)
        }
    }

    def GREATER$THAN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data > arg, DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("GREATER THAN", origin)
        }
    }

    def GREATER$THAN$OR$EQUALS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data >= arg, DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("GREATER THAN OR EQUALS", origin)
        }
    }

    // 1 to 10
    def TO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger && arg.isInteger) {
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
                List[String](data.asText, arg.asText).asJava.toDataCell(DataType.ARRAY)
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

    /* ---------- Timer ---------- */

    def NANO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("NANO=" + data.asText, DataType.TEXT)
    }

    def NANOS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("NANO=" + data.asText, DataType.TEXT)
    }

    def MICRO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("MICRO=" + data.asText, DataType.TEXT)
    }

    def MICROS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("MICRO=" + data.asText, DataType.TEXT)
    }

    def MILLI(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger.toDataCell(DataType.INTEGER)
    }

    def MILLIS(data: DataCell, arg: DataCell, origin: String): DataCell = {
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

    def MONTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("MONTH=" + data.asText, DataType.TEXT)
    }

    def MONTHS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("MONTH=" + data.asText, DataType.TEXT)
    }

    def YEAR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("YEAR=" + data.asText, DataType.TEXT)
    }

    def YEARS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell("YEAR=" + data.asText, DataType.TEXT)
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

    def TO$TIMESPAN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asInteger.toTimeSpan(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            data.asInteger.toTimeSpan().toDataCell(DataType.TEXT)
        }
    }

    /* ---------- 字符串处理 ---------- */

    def SPLIT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                val delimiter = arg.asList[String]
                data.asText.$split(delimiter.head, delimiter.last).toRow.toDataCell(DataType.ROW)
            }
            else {
                val delimiter = arg.asText
                if (delimiter != "") {
                    data.asText.split(delimiter, -1).toList.asJava.toDataCell(DataType.ARRAY)
                }
                else {
                    data.asText.split(delimiter).toList.asJava.toDataCell(DataType.ARRAY)
                }
            }
        }
        else {
            throw SharpLinkArgumentException.occur("SPLIT", origin)
        }
    }

    def DROP(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.drop(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at DROP. " + origin)
        }
    }

    def DROP$LEFT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.drop(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at DROP LEFT. " + origin)
        }
    }

    def DROP$RIGHT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.dropRight(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at DROP RIGHT. " + origin)
        }
    }

    def TAKE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.take(arg.asInteger.toInt).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE. " + origin)
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


    def TAKE$BETWEEN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val between = arg.asList
            data.asText.takeBetween(between.head.toString, between.last.toString).toDataCell(DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur(s"TAKE BETWEEN", origin)
        }
    }

    def TAKE$BEFORE$LAST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.takeBeforeLast(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE RIGHT BEFORE. " + origin)
        }
    }

    def TAKE$AFTER$LAST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.takeAfterLast(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at TAKE RIGHT AFTER. " + origin)
        }
    }

    def SUBSTR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                val list = arg.asList[Long]
                data.asText.substring(list.head.toInt - 1, list.last.toInt - 1).toDataCell(DataType.TEXT)
            }
            else {
                data.asText.substring(arg.asInteger.toInt - 1).toDataCell(DataType.TEXT)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at SUBSTR. " + origin)
        }
    }

    def STARTS$WITH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.startsWith(arg.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at STARTS WITH. " + origin)
        }
    }

    def STARTS$IGNORE$CASE$WITH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.toLowerCase().startsWith(arg.asText.toLowerCase()).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("STARTS IGNORE CASE WITH", origin)
        }
    }

    def EQUALS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isDateTime || arg.isDateTime) {
                DataCell(data.asDateTime.equals(arg.asDateTime), DataType.BOOLEAN)
            }
            else {
                DataCell(data.asText == arg.asText, DataType.BOOLEAN)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("EQUALS", origin)
        }
    }

    def EQUALS$IGNORE$CASE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asText.equalsIgnoreCase(arg.asText), DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("EQUALS IGNORE CASE", origin)
        }
    }

    def ENDS$WITH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.endsWith(arg.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at ENDS WITH. " + origin)
        }
    }

    def ENDS$IGNORE$CASE$WITH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.toLowerCase().endsWith(arg.asText.toLowerCase()).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("ENDS IGNORE CASE WITH", origin)
        }
    }

    def CONTAINS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.contains(arg.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("CONTAINS", origin)
        }
    }

    def CONTAINS$IGNORE$CASE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.toLowerCase().contains(arg.asText.toLowerCase()).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("CONTAINS IGNORE CASE", origin)
        }
    }

    def BRACKETS$WITH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isText) {
                val brackets = arg.asText
                data.asText.bracketsWith(brackets, brackets).toDataCell(DataType.BOOLEAN)
            }
            else {
                val brackets = arg.asList[String]
                data.asText.bracketsWith(brackets.head, brackets.last).toDataCell(DataType.BOOLEAN)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at BRACKETS WITH. " + origin)
        }
    }

    def BRACKET(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                val brackets = arg.asList[String]
                data.asText.bracketsWith(brackets.headOption.getOrElse(""), brackets.lastOption.getOrElse("")).toDataCell(DataType.TEXT)
            }
            else {
                val brackets = arg.asText
                data.asText.bracket(brackets, brackets).toDataCell(DataType.TEXT)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at BRACKET. " + origin)
        }
    }

    def QUOTE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.bracket(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            data.asText.bracket("'").toDataCell(DataType.TEXT)
        }
    }

    def INDEX$OF(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asText.indexOf(arg.asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at INDEX OF. " + origin)
        }
    }

    def LAST$INDEX$OF(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asText.lastIndexOf(arg.asText) + 1, DataType.INTEGER)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at LAST INDEX OF. " + origin)
        }
    }

    def CONCAT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asText.concat(arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at CONCAT. " + origin)
        }
    }

    def REPLACE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            data.asText.replace(list.head, list.last).toDataCell(DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE", origin)
        }
    }

    def REPLACE$FIRST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            val text = data.asText
            if (text.contains(list.head)) {
                DataCell(text.takeBefore(list.head) + list.last + text.takeAfter(list.head), DataType.TEXT)
            }
            else {
                data
            }
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE FIRST", origin)
        }
    }

    def REPLACE$LAST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            val text = data.asText
            if (text.contains(list.head)) {
                DataCell(text.takeBeforeLast(list.head) + list.last + text.takeAfterLast(list.head), DataType.TEXT)
            }
            else {
                data
            }
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE LAST", origin)
        }
    }

    def REPLACE$ALL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            data.asText.replaceAll(list.head, list.last).toDataCell(DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE ALL", origin)
        }
    }

    def TO$UPPER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.toUpperCase().toDataCell(DataType.TEXT)
    }

    def TO$LOWER(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.toLowerCase().toDataCell(DataType.TEXT)
    }

    def TO$UPPER$CASE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.toUpperCase().toDataCell(DataType.TEXT)
    }

    def TO$LOWER$CASE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.toLowerCase().toDataCell(DataType.TEXT)
    }

    def CAPITALIZE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.capitalize.toDataCell(DataType.TEXT)
    }

    //capitalize

    def TRIM(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                val brackets = arg.asList[String]
                data.asText.$trim(brackets.head, brackets.last).toDataCell(DataType.TEXT)
            }
            else {
                data.asText.$trim(arg.asText).toDataCell(DataType.TEXT)
            }
        }
        else {
            data.asText.trim().toDataCell(DataType.TEXT)
        }
    }

    def TRIM$LEFT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.$trimLeft(if (arg.valid) arg.asText else "").toDataCell(DataType.TEXT)
    }

    def TRIM$RIGHT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asText.$trimRight(if (arg.valid) arg.asText else "").toDataCell(DataType.TEXT)
    }

    def REPEAT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isInteger) {
            List.fill(arg.asInteger.toInt)(data.asText).asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw SharpLinkArgumentException.occur("REPEAT", origin)
        }
    }

    def CHAR$AT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isInteger) {
            data.asText.charAt(arg.asInteger.toInt - 1).toDataCell(DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur("CHAR AT", origin)
        }
    }

    def MATCHES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            arg.asText.r.findAllIn(data.asText).nonEmpty.toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("MATCHES", origin)
        }
    }

    def LIKE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            { "(?i)" + arg.asText.replace("%", """[\s\S]*""").replace("?", """[\s\S]""").bracket("^", "$") }.r.test(data.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("LIKE", origin)
        }
    }

    def NOT$LIKE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        LIKE(data, arg, origin).update(!data.value.asInstanceOf[Boolean])
    }

    //Cron Expression获取下一次匹配
    def TICK$BY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            ChronExp(data.asText).getNextTick(arg.asDateTime) match {
                case Some(dt) => DataCell(dt, DataType.DATETIME)
                case None => DataCell.NULL
            }
        }
        else {
            throw SharpLinkArgumentException.occur("TICK$BY", origin)
        }
    }

    //MD5
    def TO$MD5(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            //参数是盐值
            MD5.encrypt(data.asText, arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            MD5.encrypt(data.asText).toDataCell(DataType.TEXT)
        }
    }

    //Base64
    def TO$BASE64(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            //参数是盐值
            Base64.encode(data.asText, arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            Base64.encode(data.asText).toDataCell(DataType.TEXT)
        }
    }

    def DECODE$BASE64(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            //参数是盐值
            Base64.decode(data.asText, arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            Base64.decode(data.asText).toDataCell(DataType.TEXT)
        }
    }

    def URL$ENCODE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            java.net.URLEncoder.encode(data.asText, arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            java.net.URLEncoder.encode(data.asText, Global.CHARSET).toDataCell(DataType.TEXT)
        }
    }

    def URL$DECODE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            java.net.URLDecoder.decode(data.asText, arg.asText).toDataCell(DataType.TEXT)
        }
        else {
            java.net.URLDecoder.decode(data.asText, Global.CHARSET).toDataCell(DataType.TEXT)
        }
    }

    /* ---------- 正则表达式 ---------- */

    def TEST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asRegex.test(arg.asText).toDataCell(DataType.BOOLEAN)
        }
        else {
            throw SharpLinkArgumentException.occur("TEST", origin)
        }
    }

    def FIND$FIRST$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asRegex.findFirstIn(arg.asText) match {
                case Some(value) => DataCell(value, DataType.TEXT)
                case None => DataCell("", DataType.TEXT)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("FIND FIRST IN", origin)
        }
    }

    def FIND$FIRST$MATCH$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val list = new util.ArrayList[String]()
            data.asRegex.findFirstMatchIn(arg.asText) match {
                case Some(m) =>
                    for (i <- 0 to m.groupCount) {
                        list.add(m.group(i))
                    }
                case None =>
            }
            DataCell(list, DataType.ARRAY)
        }
        else {
            throw SharpLinkArgumentException.occur("FIND FIRST MATCH IN", origin)
        }
    }

    def FIND$ALL$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val list = new util.ArrayList[String]()
            data.asRegex.findAllIn(arg.asText).foreach(list.add)
            DataCell(list, DataType.ARRAY)
        }
        else {
            throw SharpLinkArgumentException.occur("FIND ALL IN", origin)
        }
    }

    def FIND$ALL$MATCH$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val list = new util.ArrayList[util.ArrayList[String]]()
            data.asRegex.findAllMatchIn(arg.asText).foreach(m => {
                val group = new util.ArrayList[String]()
                for (i <- 0 to m.groupCount) {
                    group.add(m.group(i))
                }
                list.add(group)
            })
            DataCell(list, DataType.ARRAY)
        }
        else {
            throw SharpLinkArgumentException.occur("FIND ALL MATCH IN", origin)
        }
    }

    def REPLACE$FIRST$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            DataCell(data.asRegex.replaceFirstIn(list.head, list.last), DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE FIRST IN", origin)
        }
    }

    def REPLACE$LAST$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isJavaList) {
            val list = arg.asList[String]
            val matches = data.asRegex.findAllIn(list.head)
            if (matches.nonEmpty) {
                val last = matches.toSeq.last
                DataCell(list.head.takeBeforeLast(last) + list.last + list.head.takeAfterLast(last), DataType.TEXT)
            }
            else {
                DataCell(list.head, DataType.TEXT)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE LAST IN", origin)
        }
    }

    def REPLACE$ALL$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid&& arg.isJavaList) {
            val list = arg.asList[String]
            DataCell(data.asRegex.replaceAllIn(list.head, list.last), DataType.TEXT)
        }
        else {
            throw SharpLinkArgumentException.occur("REPLACE ALL IN", origin)
        }
    }

    /* ---------- 数字 ---------- */

    def IF$ZERO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.asInteger == 0) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF ZERO", origin)
        }
    }

    def IF$NOT$ZERO(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.asInteger != 0) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF NOT ZERO", origin)
        }
    }

    def FLOOR(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.floor(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.floor(0).toDataCell(DataType.INTEGER)
        }
    }

    def ROUND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.round(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.round(0).toDataCell(DataType.INTEGER)
        }
    }

    def POW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asDecimal.pow(arg.asInteger.toInt).toDataCell(DataType.DECIMAL)
        }
        else {
            data.asDecimal.pow(2).toDataCell(DataType.DECIMAL)
        }
    }

    def TO$PERCENT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asDecimal.percent.toDataCell(DataType.TEXT)
    }

    def TO$CAPACITY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asInteger(0).toHumanized.toDataCell(DataType.TEXT)
    }

    def ABS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isInteger) {
            DataCell(data.asInteger(0).abs, DataType.INTEGER)
        }
        else {
            DataCell(data.asDecimal(0).abs, DataType.DECIMAL)
        }
    }

    def MULTIPLY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger) {
                if (arg.isInteger) {
                    DataCell(data.asInteger(0) * arg.asInteger(0), DataType.INTEGER)
                }
                else {
                    DataCell(data.asInteger(0) * arg.asDecimal(0), DataType.DECIMAL)
                }
            }
            else {
                DataCell(data.asDecimal(0) * arg.asDecimal(0), DataType.DECIMAL)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("MULTIPLY", origin)
        }
    }

    def DIVIDE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger) {
                if (arg.isInteger) {
                    DataCell(data.asInteger(0) / arg.asInteger(0), DataType.INTEGER)
                }
                else {
                    DataCell(data.asInteger(0) / arg.asDecimal(0), DataType.DECIMAL)
                }
            }
            else {
                DataCell(data.asDecimal(0) / arg.asDecimal(0), DataType.DECIMAL)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("DIVIDE", origin)
        }
    }

    def MOD(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger) {
                if (arg.isInteger) {
                    DataCell(data.asInteger(0) % arg.asInteger(0), DataType.INTEGER)
                }
                else {
                    DataCell(data.asInteger(0) % arg.asDecimal(0), DataType.DECIMAL)
                }
            }
            else {
                DataCell(data.asDecimal(0) % arg.asDecimal(0), DataType.DECIMAL)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("MOD", origin)
        }
    }

    def MIN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger && arg.isInteger) {
                DataCell(data.asInteger(0).min(arg.asInteger(0)), DataType.INTEGER)
            }
            else {
                DataCell(data.asDecimal(0).min(arg.asDecimal(0)), DataType.DECIMAL)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("MIN", origin)
        }
    }

    def MAX(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isInteger && arg.isInteger) {
                DataCell(data.asInteger(0).max(arg.asInteger(0)), DataType.INTEGER)
            }
            else {
                DataCell(data.asDecimal(0).max(arg.asDecimal(0)), DataType.DECIMAL)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("MAX", origin)
        }
    }

    /* ---------- 判断操作 ---------- */

    def IF$EMPTY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (
                if (data.isTable) {
                    data.asTable.isEmpty
                }
                else if (data.isRow) {
                    data.asRow.isEmpty
                }
                else if (data.isJavaList) {
                    data.asJavaList.isEmpty
                }
                else {
                    data.asText.isEmpty
                }
            ) {
                arg
            }
            else {
                data
            }
        }
        else {
            throw SharpLinkArgumentException.occur("IF EMPTY", origin)
        }
    }

    def IF$NOT$EMPTY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.asText != "") arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF NOT EMPTY", origin)
        }
    }

    def IF$NULL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.invalid || data.value == null) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF NULL", origin)
        }
    }

    def IF$NOT$NULL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.valid && data.value != null) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF NOT NULL", origin)
        }
    }

    def IF$UNDEFINED(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.dataType == DataType.EXCEPTION && data.value == "NOT_FOUND") {
                arg
            }
            else if (data.asText.removeQuotes().containsArguments) {
                arg
            }
            else {
                data
            }
        }
        else {
            throw SharpLinkArgumentException.occur("IF UNDEFINED", origin)
        }
    }

    def IF$TRUE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.asBoolean) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF TRUE", origin)
        }
    }

    def IF$FALSE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (!data.asBoolean) arg else data
        }
        else {
            throw SharpLinkArgumentException.occur("IF FALSE", origin)
        }
    }

    /* ---------- DataTable ---------- */

    def FIRST$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.firstRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (arg.valid) DataCell(arg.asRow, DataType.ROW) else throw new SharpLinkArgumentException("No result at FIRST ROW. ")
        }
    }

    def LAST$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.lastRow match {
            case Some(row) => DataCell(row, DataType.ROW)
            case None => if (arg.valid) DataCell(arg.asRow, DataType.ROW) else throw new SharpLinkArgumentException("No result at LAST ROW. ")
        }
    }

    def ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.getRow(arg.asInteger.toInt - 1).getOrElse(new DataRow()).toDataCell(DataType.ROW)
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at ROW. " + origin)
        }
    }

    def FIRST$COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.firstColumn match {
            case Some(list) => list.toJavaList.toDataCell(DataType.ARRAY)
            case None =>
                if (arg.valid) {
                    DataCell(arg.asJavaList, DataType.ARRAY)
                }
                else {
                    throw new SharpLinkArgumentException(s"No result at FIRST COLUMN. ")
                }
        }
    }

    def LAST$COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.lastColumn match {
            case Some(list) => list.toJavaList.toDataCell(DataType.ARRAY)
            case None =>
                if (arg.valid) {
                    DataCell(arg.asJavaList, DataType.ARRAY)
                }
                else {
                    throw new SharpLinkArgumentException(s"No result at LAST COLUMN. ")
                }
        }
    }

    def COLUMN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            val field = arg.asText
            val table = data.asTable
            if (table.contains(field)) {
                table.getColumn(field).toJavaList.toDataCell(DataType.ARRAY)
            }
            else {
                throw new SharpLinkArgumentException(s"No result at COLUMN: incorrect field name $field. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at COLUMN. " + origin)
        }
    }

    def FIRST$CELL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isRow) {
            data.asRow.firstCell
        }
        else {
            data.asTable.firstRow match {
                case Some(row) => row.firstCell
                case None =>
                    if (arg.valid) {
                        arg
                    }
                    else {
                        throw new SharpDataExceptionException("Empty data table at FIRST CELL. " + origin)
                    }
            }
        }
    }

    def LAST$CELL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        data.asTable.lastRow match {
            case Some(row) => row.lastCell
            case None =>
                if (arg.valid) {
                    arg
                }
                else {
                    throw new SharpDataExceptionException("Empty data table at LAST CELL. " + origin)
                }
        }
    }

    def FIRST$ROW$CELL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.firstRow match {
                case Some(row) =>
                    if (arg.isInteger) {
                        row.getCell(arg.asInteger(1).toInt - 1)
                    }
                    else {
                        row.getCell(arg.asText)
                    }
                case None => throw new SharpDataExceptionException("Empty data table at FIRST ROW CELL. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at FIRST ROW CELL. " + origin)
        }
    }

    def LAST$$ROW$CELL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asTable.lastRow match {
                case Some(row) =>
                    if (arg.isInteger) {
                        row.getCell(arg.asInteger(1).toInt - 1)
                    }
                    else {
                        row.getCell(arg.asText)
                    }
                case None => throw new SharpDataExceptionException("Empty data table at LAST ROW CELL. " + origin)
            }
        }
        else {
            throw new SharpLinkArgumentException(s"Empty or wrong argument at LAST ROW CELL. " + origin)
        }
    }

    //INSERT IF EMPTY "(id, status) VALUES (1, 'success')"
    //INSERT IF EMPTY (id, status) VALUES (1, 'success')
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
                throw SharpLinkArgumentException.occur("INSERT", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("INSERT", origin)
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
            throw SharpLinkArgumentException.occur("INSERT IF EMPTY", origin)
        }
    }

    def VALUES(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isJavaList && arg.isJavaList) {

                val fields = data.asJavaList
                val values = arg.asJavaList
                if (fields.size() == values.size()) {
                    val row = new DataRow()
                    for (i <- 0 until fields.size()) {
                        row.set(fields.get(i).toString, values.get(i))
                    }
                    DataCell(row, DataType.ROW)
                }
                else {
                    throw new SharpLinkArgumentException("Column count doesn't match value count: VALUES")
                }
            }
            else {
                DataCell(data.asText + " VALUES " + arg.asText, DataType.TEXT)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("VALUES", origin)
        }
    }

    //TURN 'a' AND 'b' TO ROW
    //TURN (a, b) TO ROW
    //TURN ['a', 'b'] TO ROW
    //TURN a, b TO ROW
    def TURN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        SELECT(data, arg, origin)
    }

    // SELECT * 无意义
    // SELECT a, b
    def SELECT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isJavaList) {
                data.asTable.select(arg.asList[String]: _*).toDataCell(DataType.TABLE)
            }
            else if (arg.isText) {
                data.asTable.select(arg.asText).toDataCell(DataType.TABLE)
            }
            else {
                throw SharpLinkArgumentException.occur("SELECT", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("SELECT", origin)
        }
    }

    def TO$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        TURN$TO$ROW(data, arg, origin)
    }

    //TURN TO ROW
    def TURN$TO$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asTable.turnToRow, DataType.ROW)
    }

    def FIELDS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asTable.getFieldNameList, DataType.ARRAY)
    }

    def LABELS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asTable.getLabelNameList, DataType.ARRAY)
    }

    def HEADERS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asTable.getHeaders, DataType.ROW)
    }

    def TO$HTML$TABLE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asTable.toHtmlString, DataType.TEXT)
    }

    //将TABLE聚合成一个LinkedHashMap
    def TO$NESTED$MAP(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isText) {
                data.asTable.turnToMap(arg.asText).toDataCell(DataType.forClassName("java.util.LinkedHashMap"))
            }
            else {
                throw SharpLinkArgumentException.occur("TO NESTED MAP", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("TO NESTED MAP", origin)
        }
    }

    /* ---------- DataRow ---------- */
    def SET(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid && arg.isRow) {
            if (data.isRow) {
                data.asRow.combine(arg.asRow)
                data
            }
            else if (data.is("io.qross.net.Cookies")) {
                //Cookie.set(arg.asRow)
                null
            }
            else if (data.is("io.qross.net.Session")) {
                //Session.set(arg.asRow)
                null
            }
            else if (data.isDateTime) {
                EXPRESS(data, arg, origin)
            }
            else {
                throw SharpInapplicableLinkNameException.occur("SET", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("SET", origin)
        }
    }

    def HAS(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isTable) {
                DataCell(data.asTable.contains(arg.asText), DataType.BOOLEAN)
            }
            else if (data.isRow) {
                DataCell(data.asRow.contains(arg.asText), DataType.BOOLEAN)
            }
            else if (data.isJavaList) {
                DataCell(data.asJavaList.contains(arg.value), DataType.BOOLEAN)
            }
            else {
                throw SharpInapplicableLinkNameException.occur("HAS", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("HAS", origin)
        }
    }

    def REMOVE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isRow) {
                if (arg.isJavaList) {
                    arg.asList[String].foreach(field => data.asRow.remove(field))
                }
                else {
                    data.asRow.remove(arg.asText)
                }
                data
            }
            else if (data.isJavaList) {
                if (arg.isJavaList) {
                    arg.asList[Int].map(_ - 1).sorted.reverse.foreach(data.asJavaList.remove)
                }
                else {
                    data.asJavaList.remove(arg.asInteger(1).toInt - 1)
                }
                data
            }
            else {
                throw SharpInapplicableLinkNameException.occur("REMOVE", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("REMOVE", origin)
        }
    }

    //TURN TO TABLE "a" AND "b"
    //TURN TO TABLE (a, b)
    //TURN TO TABLE ["a", "b"]
    //TURN TO TABLE "a","b"
    //TURN TO TABLE a, b
    def TO$TABLE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isRow) {
            if (arg.valid) {
                val args: List[String] = {
                    if (arg.isJavaList) {
                        arg.asList[String]
                    }
                    else if (arg.isText) {
                        arg.asText.$trim("(", ")").split(",").map(_.removeQuotes()).toList
                    }
                    else {
                        throw SharpLinkArgumentException.occur("TO TABLE", origin)
                    }
                }

                if (args.size >= 2) {
                    data.asRow.turnToTable(args.head, args(1)).toDataCell(DataType.TABLE)
                }
                else if (args.size == 1) {
                    data.asRow.turnToTable(args.head, "value").toDataCell(DataType.TABLE)
                }
                else {
                    data.asRow.turnToTable("key", "value").toDataCell(DataType.TABLE)
                }
            }
            else {
                data.asRow.turnToTable("key", "value").toDataCell(DataType.TABLE)
            }
        }
        else if (data.isJavaList) {
            if (arg.valid) {
                data.asTable(arg.asText.$trim("(", ")")).toDataCell(DataType.TABLE)
            }
            else {
                data.asTable.toDataCell(DataType.TABLE)
            }
        }
        else if (data.isTable) {
            data
        }
        else {
            throw SharpInapplicableLinkNameException.occur("TO TABLE", origin)
        }
    }

    /* ---------- DataList ---------- */

    def ADD(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isJavaList) {
            if (arg.valid) {
                if (arg.isJavaList) {
                    data.asJavaList.addAll(arg.asJavaList)
                }
                else {
                    data.asJavaList.add(arg.value)
                }
                data
            }
            else {
                throw SharpLinkArgumentException.occur("ADD", origin)
            }
        }
        else {
            throw SharpInapplicableLinkNameException.occur("ADD", origin)
        }
    }

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
        DataCell(data.dataType match {
            case DataType.TEXT => data.asText.length
            case DataType.ARRAY => data.asList.size
            case DataType.ROW => data.asRow.size
            case DataType.TABLE => data.asTable.size
            case _ => throw new SharpInapplicableLinkNameException("Inapplicable data type for link name SIZE. " + origin)
        }, DataType.INTEGER)
    }

    def LENGTH(data: DataCell, arg: DataCell, origin: String): DataCell = {
        SIZE(data, arg, origin)
    }

    def COUNT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.dataType match {
            case DataType.TEXT => data.asText.length
            case DataType.ARRAY => data.asList.size
            case DataType.ROW => data.asRow.size
            case DataType.TABLE => data.asTable.size
            case _ => throw new SharpInapplicableLinkNameException("Inapplicable data type for link name COUNT. " + origin)
        }, DataType.INTEGER)
    }

    def FIRST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asList.head)
    }

    def LAST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        DataCell(data.asList.last)
    }

    def GET(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (data.isJavaList) {
                val index = arg.asInteger.toInt - 1
                val list = data.asList
                if (index < list.size) {
                    DataCell(list(index))
                }
                else {
                    throw new SharpLinkArgumentException(s"Out of index at GET, index: $index. " + origin)
                }
            }
            else if (data.isRow) {
                if (arg.isInteger) {
                    data.asRow.getCell(arg.asInteger.toInt - 1)
                }
                else {
                    data.asRow.getCell(arg.asText)
                }
            }
            else if (data.isText) {
                val text = data.asText.trim()
                if (text.bracketsWith("{", "}")) {
                    if (arg.isInteger) {
                        Json(text).parseRow("/").getCell(arg.asInteger.toInt - 1)
                    }
                    else {
                        Json(text).parseRow("/").getCell(arg.asText)
                    }
                }
                else if (text.bracketsWith("[", "]")) {
                    val list = Json(text).parseJavaList("/")
                    if (arg.isInteger) {
                        val index = arg.asInteger.toInt - 1
                        if (index  < list.size()) {
                            DataCell(list.get(index))
                        }
                        else {
                            throw new SharpLinkArgumentException(s"Out of index at GET, index: $index. " + origin)
                        }
                    }
                    else {
                        throw SharpLinkArgumentException.occur("GET", origin)
                    }
                }
                else {
                    throw SharpInapplicableLinkNameException.occur("GET", origin)
                }
            }
            else {
                throw SharpInapplicableLinkNameException.occur("GET", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur("GET", origin)
        }
    }

    def REVERSE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val list = data.asList.reverse.asJava
        data.asJavaList.clear()
        data.asJavaList.addAll(list)
        data
    }

    def SORT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val list = data.asList.sortWith((a: Any, b: Any) => {
            if (a.isInstanceOf[String] || b.isInstanceOf[String]) {
                a.toString.compareTo(b.toString) < 1
            }
            else if ((a.isInstanceOf[Int] || a.isInstanceOf[Long] || a.isInstanceOf[Float] || a.isInstanceOf[Double]) && (b.isInstanceOf[Int] || b.isInstanceOf[Long] || b.isInstanceOf[Float] || b.isInstanceOf[Double])) {
                a.toDecimal(0) < b.toDecimal(0)
            }
            else {
                false
            }
        }).asJava

        data.asJavaList.clear()
        data.asJavaList.addAll(list)
        data
    }

    def ASC(data: DataCell, arg: DataCell, origin: String): DataCell = {
        SORT(data, arg, origin)
    }

    def DESC(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val list = data.asList.sortWith((a: Any, b: Any) => {
            if (a.isInstanceOf[String] || b.isInstanceOf[String]) {
                a.toString.compareTo(b.toString) >= 1
            }
            else if ((a.isInstanceOf[Int] || a.isInstanceOf[Long] || a.isInstanceOf[Float] || a.isInstanceOf[Double]) && (b.isInstanceOf[Int] || b.isInstanceOf[Long] || b.isInstanceOf[Float] || b.isInstanceOf[Double])) {
                a.toDecimal(0) >= b.toDecimal(0)
            }
            else {
                false
            }
        }).asJava

        data.asJavaList.clear()
        data.asJavaList.addAll(list)
        data
    }

    def DELIMIT(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            data.asList[String].delimit(arg.asText).toRow.toDataCell(DataType.ROW)
        }
        else {
            throw SharpLinkArgumentException.occur(s"DELIMIT", origin)
        }
    }

    def IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isText) {
                val chars = new ListBuffer[String]
                arg.asText
                   .pickChars(chars)
                   .$trim("(", ")")
                   .split(",")
                   .toSet[String]
                   .map(_.restoreChars(chars).removeQuotes())
                   .contains(data.asText.removeQuotes()).toDataCell(DataType.BOOLEAN)
            }
            else {
                arg.asList.toSet.contains(data.value).toDataCell(DataType.BOOLEAN)
            }
        }
        else {
            throw SharpLinkArgumentException.occur(s"IN", origin)
        }
    }

    def NOT$IN(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            if (arg.isText) {
                {
                    val chars = new ListBuffer[String]
                    !arg.asText
                        .pickChars(chars)
                        .$trim("(", ")")
                        .split(",")
                        .toSet[String]
                        .map(_.restoreChars(chars).removeQuotes())
                        .contains(data.asText.removeQuotes())
                }.toDataCell(DataType.BOOLEAN)
            }
            else {
                DataCell(!arg.asList.toSet.contains(data.value), DataType.BOOLEAN)
            }
        }
        else {
            throw SharpLinkArgumentException.occur(s"NOT IN", origin)
        }
    }

    /* JSON */

    def FIND(data: DataCell, arg: DataCell, origin: String): DataCell = {
        val json = data.asJson
        if (arg.isText) {
            json.parseTable(arg.asText).toDataCell(DataType.TABLE)
        }
        else if (arg.isJavaList) {
            val list = arg.asJavaList
            if (!list.isEmpty) {
                val option = list.get(0).asInstanceOf[String]
                """(?i)\sAS\s""".r.findFirstIn(option) match {
                    case Some(as) =>
                        val path = option.takeBefore(as).trim().removeQuotes()
                        option.takeAfter(as).trim() match {
                            case "ROW" => json.parseRow(path).toDataCell(DataType.ROW)
                            case "ARRAY" => json.parseJavaList(path).toDataCell(DataType.ARRAY)
                            case "VALUE" => json.parseValue(path)
                            case _ => json.parseTable(path).toDataCell(DataType.TABLE)
                        }
                    case None => json.parseTable(arg.asText).toDataCell(DataType.TABLE)
                }
            }
            else {
                throw SharpLinkArgumentException.occur(s"FIND", origin)
            }
        }
        else {
            throw SharpLinkArgumentException.occur(s"FIND", origin)
        }
    }

    def AS$TABLE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isTable) {
            data
        }
        else if (data.isText) {
            List[String](data.asText, "TABLE").asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw SharpInapplicableLinkNameException.occur("AS TABLE", origin)
        }
    }

    def AS$ROW(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isTable) {
            data.asTable.firstRow match {
                case Some(row) => DataCell(row, DataType.ROW)
                case None => DataCell.UNDEFINED
            }
        }
        else if (data.isText) {
            List[String](data.asText, "ROW").asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw SharpInapplicableLinkNameException.occur("AS TABLE", origin)
        }
    }

    def AS$ARRAY(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isTable) {
            data.asTable.firstColumn match {
                case Some(column) => DataCell(column, DataType.ROW)
                case None => DataCell.UNDEFINED
            }
        }
        else if (data.isText) {
            List[String](data.asText, "ARRAY").asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw SharpInapplicableLinkNameException.occur("AS ARRAY/LIST", origin)
        }
    }

    def AS$LIST(data: DataCell, arg: DataCell, origin: String): DataCell = {
        AS$ROW(data, arg, origin)
    }

    def AS$VALUE(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isTable) {
            data.asTable.firstColumn match {
                case Some(column) =>
                    if (column.nonEmpty) {
                        DataCell(column.head)
                    }
                    else {
                        DataCell.UNDEFINED
                    }
                case None => DataCell.UNDEFINED
            }
        }
        else if (data.isText) {
            List[String](data.asText, "VALUE").asJava.toDataCell(DataType.ARRAY)
        }
        else {
            throw SharpInapplicableLinkNameException.occur("AS ARRAY/LIST", origin)
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

    def TO$DECIMAL(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (arg.valid) {
            DataCell(data.asDecimal(arg.asDecimal), DataType.DECIMAL)
        }
        else {
            DataCell(data.asDecimal, DataType.DECIMAL)
        }
    }

    def RANDOM(data: DataCell, arg: DataCell, origin: String): DataCell = {
        if (data.isText) {
            data.asText.shuffle(if (arg.valid) arg.asInteger(1).toInt else 1).toDataCell(DataType.TEXT)
        }
        else if (data.isJavaList) {
            val list = data.asJavaList
            val length = list.size()
            //如从一个区间一个随机数
            if (arg.invalid || arg.asInteger(1) == 1) {
                DataCell(list.get(Random.nextInt(length)))
            }
            //从一个区间取多个随机数
            else {
                val sample = new util.ArrayList[Any]()
                for (i <- 1 to arg.asInteger(1).toInt) {
                    sample.add(list.get(Random.nextInt(length)))
                }
                DataCell(sample, DataType.ARRAY)
            }
        }
        else if (data.isTable) {
            data.asTable.takeSample(if (arg.valid) arg.asInteger(1).toInt else 1).toDataCell(DataType.TABLE)
        }
        else {
            throw SharpInapplicableLinkNameException.occur("RANDOM", origin)
        }
    }
}

//expression已cleaned
class Sharp(private val expression: String, private var data: DataCell = DataCell.ERROR) {

    //LET @NOW EXPRESS "DAY=1#DAY-1" FORMAT "yyyyMMdd" TO DECIMAL # ROUND # POW 2

    /*
    SHARP表达式以LET开头, LET在独立语句中不可以省略
    处理数据的方法称为“连接”
    连接可以没有参数或有一个参数
    没有参数的多个连接之间必须使用 # 或 -> 或 回车换行隔开
     */

    // VALUE  v > l
    // VALUE LINK  v = l
    // VALUE LINK ARG  v > l

    //无参访求用于不需要PQL的场景, Keeper中有用到
    def execute(): DataCell = {

        var sentence = " " + expression

        //分组数据即(..)包围的数据如 (1, "2")，去掉空白字符
        //SET a=1, b = 2, c=3 去掉空白字符
        //SELECT a, b, c, d 去掉空白字符
        $ARGS.findAllIn(sentence).foreach(comma => {
            sentence = sentence.replace(comma, comma.trim())
        })

        //处理 AS
        $AS.findAllIn(sentence).foreach(as => {
            sentence = sentence.replace(as, "##AS##")
        })

        val links = new mutable.ListBuffer[Link$Argument]()

        val matches = $LINK.findAllIn(sentence).toArray

        //恢复 AS
        sentence = sentence.replace("##AS##", " AS ")

        for (i <- matches.indices) {
            if (i == matches.length - 1) {
                links ++= SharpLink.parse(matches(i), sentence.takeAfter(matches(i)))
            }
            else {
                sentence = sentence.takeAfter(matches(i))
                links ++= SharpLink.parse(matches(i), sentence.takeBefore(matches(i+1)))
            }
        }

        for (i <- links.indices) {
            if (SharpLink.all.contains(links(i).linkName)) {
                //必须是等号, 不能用replace方法, 否则变量内容会保存
                data = classOf[Sharp]
                        .getDeclaredMethod(links(i).linkName, classOf[DataCell], classOf[DataCell], classOf[String])
                        .invoke(null,
                            data,
                            if (i + 1 < links.length &&
                                (SharpLink.priorities.contains(links(i).linkName) &&
                                    SharpLink.priorities(links(i).linkName).contains(links(i+1).linkName)) //prioritization of execution
                            ) {
                                val name = links(i+1).linkName
                                links(i+1).linkName = "" //mark as executed
                                classOf[Sharp]
                                    .getDeclaredMethod(name, classOf[DataCell], classOf[DataCell], classOf[String])
                                    .invoke(null,
                                        links(i).solve(),
                                        links(i+1).solve(),
                                        links(i+1).originate()
                                    )
                            }
                            else {
                                links(i).solve()
                            },
                            links(i).originate()
                        ).asInstanceOf[DataCell]
            }
            else if (links(i).linkName != "") {
                throw new SharpLinkArgumentException("Wrong link name: " + links(i).originalLinkName)
            }
        }

        data
    }

    def execute(PQL: PQL): DataCell = {

        var sentence = {
            if (data.invalid) {
                if (expression.startsWith(ARROW)) {
                    expression.substring(2).trim()
                }
                else {
                    expression.takeAfterX($LET).trim()
                }
            }
            else {
                " " + expression
            }
        }

        //分组数据即(..)包围的数据如 (1, "2")，去掉空白字符
        //SET a=1, b = 2, c=3 去掉空白字符
        //SELECT a, b, c, d 去掉空白字符
        $ARGS.findAllIn(sentence).foreach(comma => {
            sentence = sentence.replace(comma, comma.trim())
        })

        //处理 AS, Sharp LINK 中避免使用 AS 关键词
        $AS.findAllIn(sentence).foreach(as => {
            sentence = sentence.replace(as, "##AS##")
        })

        val links = new mutable.ListBuffer[Link$Argument]()

        val matches = $LINK.findAllIn(sentence).toArray

        //恢复 AS
        sentence = sentence.replace("##AS##", " AS ")

        if (data.invalid) {
            //必须是等号, 不能用replace方法, 否则变量内容会保存
            data = {
                if (matches.nonEmpty) {
                    sentence.takeBefore(matches.head).$sharp(PQL)
                }
                else {
                    sentence.$sharp(PQL)
                }
            }
        }

        for (i <- matches.indices) {
            if (i == matches.length - 1) {
                links ++= SharpLink.parse(matches(i), sentence.takeAfter(matches(i)))
            }
            else {
                sentence = sentence.takeAfter(matches(i))
                links ++= SharpLink.parse(matches(i), sentence.takeBefore(matches(i+1)))
            }
        }

        for (i <- links.indices) {
            if (SharpLink.all.contains(links(i).linkName)) {
                //必须是等号, 不能用replace方法, 否则变量内容会保存
                //这里不能使用 Sharp.getClass
                data = classOf[Sharp]
                            .getDeclaredMethod(links(i).linkName, classOf[DataCell], classOf[DataCell], classOf[String]) //classOf[DataCell], classOf[DataCell], classOf[String]  Class.forName("io.qross.core.DataCell"), Class.forName("io.qross.core.DataCell"), Class.forName("java.lang.String")
                                .invoke(null,
                                    data,
                                    if (i + 1 < links.length &&
                                            (SharpLink.priorities.contains(links(i).linkName) &&
                                                SharpLink.priorities(links(i).linkName).contains(links(i+1).linkName)) //prioritization of execution
                                    ) {
                                        val name = links(i+1).linkName
                                        links(i+1).linkName = "" //mark as executed
                                        classOf[Sharp]
                                            .getDeclaredMethod(name, classOf[DataCell], classOf[DataCell], classOf[String])
                                                .invoke(null,
                                                    links(i).solve(PQL),
                                                    links(i+1).solve(PQL),
                                                    links(i+1).originate(PQL)
                                                )
                                    }
                                    else {
                                        links(i).solve(PQL)
                                    },
                                    links(i).originate(PQL)
                                ).asInstanceOf[DataCell]
            }
            else if (links(i).linkName != "") {
                throw new SharpLinkArgumentException("Wrong link name: " + links(i).originalLinkName)
            }
        }

        data
    }
}

object SharpLink {
    val all: Set[String] = Sharp.getClass.getDeclaredMethods.map(m => m.getName).filter(n => """^[A-Z][A-Z\$]*[A-Z\d]+$|^[A-Z]$""".r.test(n)).toSet
    val priorities: Map[String, Set[String]] =
        Map[String, Set[String]](
            "SUBSTR" -> Set[String]("TO"),
            "SPLIT" -> Set[String]("AND"),
            "BRACKETS$WITH" -> Set[String]("AND"),
            "TAKE$BETWEEN" -> Set[String]("AND"),
            "BRACKET" -> Set[String]("AND"),
            "REPLACE" -> Set[String]("TO"),
            "REPLACE$FIRST" -> Set[String]("TO"),
            "REPLACE$ALL" -> Set[String]("TO"),
            "REPLACE$ALL$IN" -> Set[String]("TO"),
            "REPLACE$FIRST$IN" -> Set[String]("TO"),
            "REPLACE$LAST$IN" -> Set[String]("TO"),
            "ADD" -> Set[String]("TO", "UNTIL"),
            "TRIM" -> Set[String]("AND"),
            "PLUS" -> Set[String]("YEAR", "YEARS", "MONTH", "MONTHS", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MICRO", "MICROS", "NANO", "NANOS"),
            "MINUS" -> Set[String]("YEAR", "YEARS", "MONTH", "MONTHS", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "MILLI", "MILLIS", "MICRO", "MICROS", "NANO", "NANOS"),
            "INSERT" -> Set[String]("VALUES"),
            "INSERT$IF$EMPTY" -> Set[String]("VALUES"),
            "TURN" -> Set[String]("AND"))
    //有可能属性值是一个参数（至少有一个参数，不能为空参数）且参数可能是英文单词的link名称
    val columns: Set[String] = Set[String]("SELECT", "REMOVE", "ORDER$BY", "GROUP$BY")

    def format(linkName: String): String = {
        linkName.trim().replaceAll(BLANKS, "\\$").toUpperCase()
    }

    def parse(link: String, args: String): mutable.ArrayBuffer[Link$Argument] = {

        val links = new mutable.ArrayBuffer[Link$Argument]()
        var linkName = SharpLink.format(link)

        if (SharpLink.all.contains(linkName)) {
            links += new Link$Argument(link, args)
        }
        else if (linkName.contains("$")) {
            //一种是 SELECT / DELETE 增link只有一个字段
            //二是两个或多个无参link合在了一起

            //解析原理
            // 一个单词一个单词向后退，直到找到匹配
            //找到之后剩下的继续解析，同样一个单词一个单词向后退

            var origin = link.trim().takeBeforeLastX(Patterns.$BLANK).trim()
            var more = link.trim().takeAfterLastX(Patterns.$BLANK).trim()
            linkName = linkName.takeBeforeLast("$")

            while (origin != "") {
                if (SharpLink.all.contains(linkName)) {
                    if (SharpLink.columns.contains(linkName)) {
                        Patterns.$BLANK.findFirstIn(more) match {
                            case Some(blank) =>
                                links += new Link$Argument(origin, more.takeBefore(blank))

                                origin = more.takeAfter(blank).trim
                                more = ""
                                linkName = SharpLink.format(origin)
                            case None =>
                                if (more != "") {
                                    links += new Link$Argument(origin, more)
                                    origin = ""
                                    if (args.trim != "") {
                                        throw new SharpLinkArgumentException("Wrong link name: " + link + " or redundant args: " + args)
                                    }
                                }
                                else {
                                    links += new Link$Argument(origin, args)
                                }
                        }
                    }
                    else if (more != "") {
                        links += new Link$Argument(origin, "")

                        origin = more
                        more = ""
                        linkName = SharpLink.format(origin)
                    }
                    else {
                        links += new Link$Argument(origin, args)
                        origin = ""
                    }
                }
                else if (linkName.contains("$")) {
                    more = (origin.takeAfterLastX(Patterns.$BLANK) + " " + more).trim()
                    origin = origin.takeBeforeLastX(Patterns.$BLANK).trim()
                    linkName = linkName.takeBeforeLast("$")
                }
                else {
                    throw new SharpLinkArgumentException("Wrong link name: " + link)
                }
            }
        }
        else {
            throw new SharpLinkArgumentException("Wrong link name: " + link)
        }

        links
    }
}

class Link$Argument(val originalLinkName: String, val originalArgument: String) {
    var linkName: String = SharpLink.format(originalLinkName)
    val argument: String = {
        if (originalArgument.trim().endsWith("->")) {
            originalArgument.takeBeforeLast("->").trim()
        }
        else {
            originalArgument.trim()
        }
    }

    def solve(): DataCell = {
        if (argument != "") {
            if (""",|\sAS\s|^\*$""".r.test(argument)) {
                //SET或SELECT
                if (argument.contains("=")) {
                    val row = new DataRow()
                    argument
                        .split(",")
                        .foreach(item => {
                            if (item.contains("=")) {
                                row.set(item.takeBefore("="), item.takeAfter("="))
                            }
                            else {
                                row.set(item, null, DataType.NULL)
                            }
                        })
                    DataCell(row, DataType.ROW)
                }
                else {
                    DataCell(argument.split(",").map(_.removeQuotes()).toList.asJava, DataType.ARRAY)
                }
            }
            else {
                DataCell(argument.removeQuotes(), DataType.TEXT)
            }
        }
        else {
            DataCell.NULL
        }
    }

    def solve(PQL: PQL): DataCell = {
        if (argument != "") {
            if ("""(?i),|\sAS\s|^\*$""".r.test(argument)) {
                //SET或SELECT或VALUES
                if (argument.contains("=")) {
                    val row = new DataRow()
                    //SET
                    argument
                        .split(",")
                        .foreach(item => {
                            if (item.contains("=")) {
                                row.set(item.takeBefore("=").popStash(PQL), item.takeAfter("=").$sharp(PQL))
                            }
                            else {
                                row.set(item, null, DataType.NULL)
                            }
                        })
                    DataCell(row, DataType.ROW)
                }
                else {
                    //SELECT/FIND/VALUES
                    DataCell(
                        argument
                            .$trim("(", ")")
                            .split(",")
                            .map(item =>
                                if ("""(?i)\sAS\s""".r.test(item)) {
                                    item.popStash(PQL)
                                }
                                else {
                                    item.$sharp(PQL).value
                                }
                            ).toList.asJava, DataType.ARRAY)
                }
            }
            else {
                argument.$trim("(", ")").$sharp(PQL)
            }
        }
        else {
            DataCell.NULL
        }
    }

    def originate(): String = {
        originalLinkName

    }

    def originate(PQL: PQL): String = {
        if (argument != "") {
            originalLinkName + " " + originalArgument.popStash(PQL)
        }
        else {
            originalLinkName
        }
    }
}
