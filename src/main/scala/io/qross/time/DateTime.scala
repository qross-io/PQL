package io.qross.time

import java.sql.{Date, Time, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}
import java.util.regex.Pattern

import io.qross.ext.Output
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DateTime {
    
    def now: DateTime = new DateTime()
    def from(dateTime: DateTime): DateTime = new DateTime(dateTime.localDateTime)
    def ofTimestamp(epochSecond: Long): DateTime = new DateTime(epochSecond.toString, "EPOCH")
    def of(year: Int, month: Int, dayOfMonth: Int, hourOfDay: Int = 0, minute: Int = 0, second: Int = 0): DateTime = {
        new DateTime(f"$year-$month%02d-$dayOfMonth%02d $hourOfDay%02d:$minute%02d:$second%02d", "yyyy-MM-dd HH:mm:ss")
    }
    
    def getDaysSpan(beginTime: String, endTime: String): Long = getDaysSpan(new DateTime(beginTime), new DateTime(endTime))
    def getDaysSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.DAYS.between(beginTime.localDateTime, endTime.localDateTime)
    def getSecondsSpan(beginTime: String, endTime: String): Long = getSecondsSpan(new DateTime(beginTime), new DateTime(endTime))
    def getSecondsSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.SECONDS.between(beginTime.localDateTime, endTime.localDateTime)
}

class  DateTime(private val dateTime: Any = "", private val formatStyle: String = "") {
    
    //E,EE,EEE = Sun
    //EEEE = Sunday
    //e = 1
    //ee = 01
    //eee = Sun
    //eeee = Sunday
    
    //M = 11
    //MM = 11
    //MMM = Nov
    //MMMM = November
    
    //a = PM

    val localDateTime: LocalDateTime = dateTime match {
        case localDateTime: LocalDateTime => localDateTime
        case str: String => if (str == "") {
                                LocalDateTime.now()
                            }
                            else {
                                parseLocalDateTime(str, formatStyle)
                            }
        case dt: DateTime => dt.localDateTime
        case date: Date => parseLocalDateTime(date.toString + " 00:00:00", "yyyy-MM-dd HH:mm:ss")
        case time: Time => parseLocalDateTime(LocalDate.now() + " " + time.toString, "yyyy-MM-dd HH:mm:ss")
        case timeStamp: Timestamp => timeStamp.toLocalDateTime
        case l: Long => parseLocalDateTime(l)
        case i: Int => parseLocalDateTime(i)
        case localDate: LocalDate => parseLocalDateTime(localDate.toString + " 00:00:00", "yyyy-MM-dd HH:mm:ss")
        case localTime: LocalTime => parseLocalDateTime(LocalDate.now() + " " + localTime.toString, "yyyy-MM-dd HH:mm:ss")
        case _ =>   Output.writeWarning("Can't recognize as or convert to DateTime: " + dateTime)
                    LocalDateTime.now()
    }

    private def parseLocalDateTime(dateTime: String, format: String = ""): LocalDateTime = {

        val style = {
            if (format == "") {
                dateTime.length match {
                    case 8 =>
                        if (dateTime.contains(":")) {
                            "HH:mm:ss"
                        }
                        else {
                            "yyyyMMdd"
                        }
                    case 10 =>
                        if (dateTime.contains("-")) {
                            "yyyy-MM-dd"
                        } else if (dateTime.contains("/")) {
                            if (dateTime.indexOf("/") == 4) {
                                "yyyy/MM/dd"
                            }
                            else {
                                "dd/MM/yyyy"
                            }
                        }
                        else {
                            "yyyyMMddHH"
                        }
                    case 12 => "yyyyMMddHHmm"
                    case 14 => "yyyyMMddHHmmss"
                    case 16 => "yyyyMMddHHmmss.S"
                    case 17 => "yyyyMMddHHmmss.SS"
                    case 18 => "yyyyMMddHHmmss.SSS"
                    case 19 => "yyyy-MM-dd HH:mm:ss"
                    case 21 => "yyyy-MM-dd HH:mm:ss.S"
                    case 22 => "yyyy-MM-dd HH:mm:ss.SS"
                    case 23 => "yyyy-MM-dd HH:mm:ss.SSS"
                    case _ => "yyyy-MM-dd HH:mm:ss"
                }
            }
            else {
                format
            }
        }

        if (dateTime.length == 8) {
            if (style == "yyyyMMdd") {
                LocalDateTime.parse(dateTime + "00", DateTimeFormatter.ofPattern(style + "HH"))
            }
            else {
                LocalDateTime.parse( LocalDate.now().toString + " " + dateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd " + style))
            }
        }
        else if (dateTime.length == 10 && (dateTime.contains("-") || dateTime.contains("/"))) {
            LocalDateTime.parse(dateTime + " 00", DateTimeFormatter.ofPattern(style + " HH"))
        }
        else {
            LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern(style))
        }
    }

    private def parseLocalDateTime(timestamp: Long): LocalDateTime = {
        val (second: Long, nano: Long) = {
            //10位 秒
            if (timestamp < 10000000000L) {
                (timestamp, 0)
            }
            //17位 纳秒
            else if (timestamp > 9999999999999L) {
                (timestamp / 10000000, timestamp % 10000000)
            }
            //13位 毫秒
            else {
                (timestamp / 1000, timestamp % 1000 * 10000)
            }
        }
        LocalDateTime.ofEpochSecond(second, nano.toInt, OffsetDateTime.now.getOffset)
    }
   
    def get(field: ChronoField): Int = this.localDateTime.get(field)
    def getYear: Int = this.localDateTime.getYear
    def getMonth: Int = this.localDateTime.getMonthValue
    def getWeekName: String = this.getString("EEE")
    def getDayOfWeek: Int = this.localDateTime.getDayOfWeek.getValue
    def getDayOfMonth: Int = this.localDateTime.getDayOfMonth
    def getHour: Int = this.localDateTime.getHour
    def getMinute: Int = this.localDateTime.getMinute
    def getSecond: Int = this.localDateTime.getSecond
    def getNano: Int = this.localDateTime.getNano
    def getTickValue: String = this.getString("yyyyMMddHHmm00")

    def year: Int = this.localDateTime.getYear
    def month: Int = this.localDateTime.getMonthValue
    def weekName: String = this.getString("EEE")
    def dayOfWeek: Int = this.localDateTime.getDayOfWeek.getValue
    def dayOfMonth: Int = this.localDateTime.getDayOfMonth
    def hour: Int = this.localDateTime.getHour
    def minute: Int = this.localDateTime.getMinute
    def second: Int = this.localDateTime.getSecond
    def nano: Int = this.localDateTime.getNano
    def tickValue: String = this.getString("yyyyMMddHHmm00")
    
    def set(field: ChronoField, value: Int): DateTime = {
        new DateTime(this.localDateTime.`with`(field, value))
    }

    def setYear(year: Int): DateTime = {
        new DateTime(this.localDateTime.withYear(year))
    }

    def setMonth(value: Int): DateTime = {
        new DateTime(this.localDateTime.withMonth(value))
    }
    def setDayOfMonth(value: Int): DateTime = {
        new DateTime(this.localDateTime.withDayOfMonth(value))
        /*
        LocalDateTime	with(TemporalField field, long newValue)
        LocalDateTime	withDayOfMonth(int dayOfMonth)
        LocalDateTime	withDayOfYear(int dayOfYear)
        LocalDateTime	withHour(int hour)
        LocalDateTime	withMinute(int minute)
        LocalDateTime	withMonth(int month)
        LocalDateTime	withNano(int nanoOfSecond)
        LocalDateTime	withSecond(int second)
        LocalDateTime	withYear(int year)
        */
    }

    def setDayOfWeek(value: String): DateTime = {
        //MON, TUE, WED, THU, FRI, SAT, SUN
        if (value.length >= 2) {
            new DateTime(this.localDateTime.`with`(ChronoField.DAY_OF_WEEK, {
                value.substring(0, 2).toUpperCase() match {
                    case "MO" => 1
                    case "TU" => 2
                    case "WE" => 3
                    case "TH" => 4
                    case "FR" => 5
                    case "SA" => 6
                    case "SU" => 7
                    case _ => throw new IllegalArgumentException("Unknown Week name: " + value)
                }
            }))
        }
        else {
            throw new IllegalArgumentException("Week name must be 2 or more chars at least, please check.")
        }
    }
    def setDayOfWeek(value: Int): DateTime = {
        new DateTime(this.localDateTime.`with`(ChronoField.DAY_OF_WEEK, value))
    }
    def setHour(value: Int): DateTime = {
        new DateTime(this.localDateTime.withHour(value))
    }
    def setMinute(value: Int): DateTime = {
        new DateTime(this.localDateTime.withMinute(value))
    }
    def setSecond(value: Int): DateTime = {
        new DateTime(this.localDateTime.withSecond(value))
    }
    def setNano(value: Int): DateTime = {
        new DateTime(this.localDateTime.withNano(value))
    }
    
    def setZeroOfMonth(): DateTime = {
        new DateTime(this.localDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0))
    }
    def setZeroOfDay(): DateTime = {
        new DateTime(this.localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0))
    }
   
    def toEpochSecond: Long = this.localDateTime.atZone(ZoneId.systemDefault).toInstant.getEpochSecond
    
    def plus(unit: ChronoUnit, amount: Int): DateTime = {
        new DateTime(this.localDateTime.plus(amount, unit))
    }
    def plusYears(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusYears(amount))
    }
    def plusMonths(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusMonths(amount))
    }
    def plusDays(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusDays(amount))
    }
    def plusHours(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusHours(amount))
    }
    def plusMinutes(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusMinutes(amount))
    }
    def plusSeconds(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusSeconds(amount))
    }
    
    def minus(unit: ChronoUnit, amount: Int): DateTime = {
        new DateTime(this.localDateTime.minus(amount, unit))
    }
    def minusYears(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusYears(amount))
    }
    def minusMonths(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusMonths(amount))
    }
    def minusDays(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusDays(amount))
    }
    def minusHours(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusHours(amount))
    }
    def minusMinutes(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusMinutes(amount))
    }
    def minusSeconds(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusSeconds(amount))
    }
    
    def getString(formatStyle: String): String = {
        if (formatStyle.toUpperCase() != "EPOCH") {
            DateTimeFormatter.ofPattern(formatStyle).format(localDateTime)
        }
        else {
            this.toEpochSecond.toString
        }
    }
    def format(formatStyle: String): String = this.getString(formatStyle)
    def getNumber(formatStyle: String, defaultValue: Long = 0): Long = Try(this.getString(formatStyle).toLong).getOrElse(defaultValue)
    
    def equals(otherDateTime: DateTime): Boolean = this.localDateTime.isEqual(otherDateTime.localDateTime)
    def before(otherDateTime: DateTime): Boolean = this.localDateTime.isBefore(otherDateTime.localDateTime)
    def beforeOrEquals(otherDateTime: DateTime): Boolean = this.localDateTime.isBefore(otherDateTime.localDateTime) || this.localDateTime.isEqual(otherDateTime.localDateTime)
    def after(otherDateTime: DateTime): Boolean = this.localDateTime.isAfter(otherDateTime.localDateTime)
    def afterOrEquals(otherDateTime: DateTime): Boolean = this.localDateTime.isAfter(otherDateTime.localDateTime) || this.localDateTime.isEqual(otherDateTime.localDateTime)
    
    def copy(): DateTime = {
        new DateTime(this.localDateTime)
    }
    
    def copy(dateTime: DateTime): DateTime = {
        new DateTime(dateTime.localDateTime)
    }
    
    //---------- Sharp Expression ----------
    
    def shift(field: String, value: String): DateTime = {
        if (value.startsWith("-") || value.startsWith("+")) {
            //+, -
            (field.toUpperCase(), Try(value.toInt)) match {
                case ("DAY", Success(v)) => this.plusDays(v)
                case ("MONTH", Success(v)) => this.plusMonths(v)
                case ("YEAR", Success(v)) => this.plusYears(v)
                case ("HOUR", Success(v)) => this.plusHours(v)
                case ("MINUTE", Success(v)) => this.plusMinutes(v)
                case ("SECOND", Success(v)) => this.plusSeconds(v)
                case _ => this
            }
        }
        else {
            //=
            (field.toUpperCase(), Try(value.toInt)) match {
                case ("DAY", Success(v)) => if (v > 0) this.setDayOfMonth(v) else this.setDayOfMonth(1)
                case ("DAY", Failure(_)) =>  if (value == "L") this.plusMonths(1).setDayOfMonth(1).plusDays(-1) else this.setDayOfWeek(value)  // L = last_day & WeekName = Mon,Tus...Sun
                case ("WEEK", Success(v)) => if (v < 0) this.setDayOfWeek(1) else if (v > 7) this.setDayOfWeek(7) else this.setDayOfWeek(v)
                case ("WEEK", Failure(_)) => this.setDayOfWeek(value)
                case ("MONTH", Success(v)) => this.setMonth(v)
                case ("YEAR", Success(v)) => this.setYear(v)
                case ("HOUR", Success(v)) => this.setHour(v)
                case ("MINUTE", Success(v)) => this.setMinute(v)
                case ("SECOND", Success(v)) => this.setSecond(v)
                case _ => this
            }
        }
    }
    
    def express(expression: String): DateTime = {
        // Second|Minute|Hour|Day|Month|Year +|-|= num
        // exp includes num|Sun-Sat:1-7(only in day section)
        //year=2018#month=2#day=MON
        //month+1#day=1#day=MON#day-1
        var dateTime = this
        val sections = expression.toUpperCase().replaceAll("""\s""", "").replace("-", "=-").replace("+", "=+").split("#")
        for (section <- sections) {
            if (section.contains("=")) {
                //dateTime = this.shift(section.substring(0, section.indexOf("=")), section.substring(section.indexOf("=") + 1))
                dateTime = dateTime.shift(section.takeBefore("="), section.takeAfter("="))
            }
        }
        
        dateTime
    }
    
    def matches(cronExp: String): Boolean = CronExp(cronExp).matches(this)
    
    //YEAR=1970-2099,m/n
    //MONTH=1-12,FEB-DEC,m/n
    //DAY=1-31,L,W,MON,THU,WED,TUR,FRI,SAT,SUN,LW,L<WEEK>,m/n
    //HOUR=0-23,m/n
    //MINUTE=0-59,m/n
    //SECOND=0-59,m/n
    
    def shape(FROM: String, TO: String, STEP: ChronoUnit = ChronoUnit.DAYS, FILTER: String = ""):

    List[DateTime] = {
        
        val list = new mutable.LinkedHashSet[DateTime]()
        
        val begin = this.copy().express(FROM)
        val end = this.copy().express(TO)
        if (begin.before(end)) {
            while (begin.beforeOrEquals(end)) {
                list += begin.copy()
                begin.plus(STEP, 1)
            }
        }
        else if (begin.after(end)) {
            while (begin.afterOrEquals(end)) {
                list += begin.copy()
                begin.minus(STEP, 1)
            }
        }
        else {
            list += begin.copy()
        }
        
        if (FILTER.nonEmpty) {
            val cronExp =
                if (FILTER.contains("=")) {
                    val exp = mutable.LinkedHashMap[String, String](
                        "SECOND" -> "*",
                        "MINUTE" -> "*",
                        "HOUR" -> "*",
                        "DAY" -> "*",
                        "MONTH" -> "*",
                        "WEEK" -> "?",
                        "YEAR" -> "*")
                    FILTER.replace(" ", "").toUpperCase().split("#").foreach(item => {
                        if (item.contains("=")) {
                            val field = item.takeBefore("=")
                            val value = item.takeAfter("=")
                            if (value.nonEmpty) {
                                exp.put(field, value)
                                if (field == "WEEK") {
                                    exp.put("DAY", "?")
                                }
                                else if (field == "DAY") {
                                    exp.put("WEEK", "?")
                                }
                            }
                        }
                    })
                    
                    CronExp(exp.values.mkString(" "))
                }
                else {
                    CronExp(FILTER.toUpperCase())
                }
            
            //final filter
            list.filter(item => cronExp.matches(item)).toList
        }
        else {
            list.toList
        }
    }
    
    //FOR $datetime <- DAY-1 TO DAY+5 FILTER WEEK=MON#MINUTE=0/5 FORMAT yyyyMMdd
    //SET T1 := NOW -> DAY-1#HOUR=0 FORMAT yyyyMMdd
    
    //(FROM) DAY-1 TO DAY+5 FILTER DAY=1,2,3,7,L,W#WEEK=MON,FRI,LSUN#MINUTE=0/5#HOUR=1-10 FORMAT yyyyMMdd JOIN WITH ","
    //(SET) MONTH-1#DAY=1 FORMAT yyyyMMdd
    //MONTH-1#DAY=1 -> yyyyMMdd
    //(FORMAT) yyyMMddHHmmss/epoch
    
    //DateTime1.to(DateTime2): List[DateTime].filter(): List[DateTime].format(): List[String].join(): String
    //set = express(): DateTime.format(): String
    //format(): String

    def shark(expression: String): List[String] = {
        //semi-value
        var semi = expression.trim.replace("->", "FORMAT")

        var FROM: String = ""
        var TO: String = ""
        var FILTER: String = ""
        var FORMAT: String = "yyyy-MM-dd HH:mm:ss"

        //FORMAT
        if (semi.toUpperCase().contains("FORMAT")) {
            FORMAT = semi.substring(semi.toUpperCase().indexOf("FORMAT") + 6).trim
            semi = semi.substring(0, semi.toUpperCase().indexOf("FORMAT")).trim
        }

        semi = semi.toUpperCase()

        //FILTER
        if (semi.contains("FILTER")) {
            FILTER = semi.substring(semi.indexOf("FILTER") + 6).trim
            semi = semi.substring(0, semi.indexOf("FILTER")).trim
        }

        //TO
        if (semi.contains("TO")) {
            FROM = semi.substring(0, semi.indexOf("TO")).trim
            TO = semi.substring(semi.indexOf("TO") + 2).trim
            if (FROM.startsWith("FROM")) {
                FROM = FROM.drop(4).trim
            }
        }

        if (FROM.nonEmpty && TO.nonEmpty) {
            val STEP =
                if (FORMAT.contains("s")) {
                    ChronoUnit.SECONDS
                }
                else if (FORMAT.contains("m")) {
                    ChronoUnit.MINUTES
                }
                else if (FORMAT.contains("H") || FORMAT.contains("h")) {
                    ChronoUnit.HOURS
                }
                else if (FORMAT.contains("d") || FORMAT.contains("D") || FORMAT.contains("e") || FORMAT.contains("E")) {
                    ChronoUnit.DAYS
                }
                else if (FORMAT.contains("M")) {
                    ChronoUnit.MONTHS
                }
                else if (FORMAT.contains("y") || FORMAT.contains("Y")) {
                    ChronoUnit.YEARS
                }
                else {
                    ChronoUnit.DAYS
                }

            this.shape(FROM, TO, STEP, FILTER).map(dateTime => {
                if (FORMAT.nonEmpty) {
                    dateTime.format(FORMAT)
                }
                else {
                    dateTime.toString
                }
            })
        }
        else {
            //return empty if format is incorrect.
            List(sharp(expression))
        }
    }

    def sharp(expression: String): String = {
        //semi-value
        var semi = expression.trim.replace("->", "FORMAT")

        if (Pattern.compile("\\sTO\\s", Pattern.CASE_INSENSITIVE).matcher(semi).find()) {
            shark(semi).mkString(",")
        }
        else {
            var FORMAT: String = "yyyy-MM-dd HH:mm:ss"
            var SET: String = ""

            //FORMAT
            if (semi.toUpperCase().contains("FORMAT")) {
                FORMAT = semi.substring(semi.toUpperCase().indexOf("FORMAT") + 6).trim
                semi = semi.substring(0, semi.toUpperCase().indexOf("FORMAT")).trim
            }
            else {
                FORMAT = semi
                semi = ""
            }

            semi = semi.toUpperCase()

            SET = semi.trim
            if (SET.startsWith("SET")) {
                SET = SET.drop(3).trim
            }

            if (SET.nonEmpty) {
                this.express(SET)
            }
            if (FORMAT.nonEmpty) {
                this.format(FORMAT)
            }
            else {
                this.toString
            }
        }
    }
    
    override def equals(other: Any): Boolean = {
        this.getString("yyyyMMddHHmmssSSS") == other.asInstanceOf[DateTime].getString("yyyyMMddHHmmssSSS")
    }
    
    override def toString: String = {
        this.getString("yyyy-MM-dd HH:mm:ss.SSS")
    }
}