package cn.qross.time

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit, WeekFields}
import java.util.regex.Pattern

import cn.qross.core.DataRow
import cn.qross.exception.ConvertFailureException
import cn.qross.ext.TypeExt._
import cn.qross.setting.Global

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._
import scala.collection.JavaConverters._

object DateTime {
    
    def now: DateTime = new DateTime()
    def today: DateTime = DateTime.now.setZeroOfDay()
    def from(dateTime: DateTime): DateTime = new DateTime(dateTime.localDateTime)
    def ofTimestamp(epochSecond: Long): DateTime = new DateTime(epochSecond)
    def of(year: Int, month: Int, dayOfMonth: Int, hourOfDay: Int = 0, minute: Int = 0, second: Int = 0): DateTime = {
        new DateTime(f"$year-$month%02d-$dayOfMonth%02d $hourOfDay%02d:$minute%02d:$second%02d", "yyyy-MM-dd HH:mm:ss")
    }
    
    def getDaysSpan(beginTime: String, endTime: String): Long = getDaysSpan(new DateTime(beginTime), new DateTime(endTime))
    def getDaysSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.DAYS.between(beginTime.localDateTime, endTime.localDateTime)
    def getSecondsSpan(beginTime: String, endTime: String): Long = getSecondsSpan(new DateTime(beginTime), new DateTime(endTime))
    def getSecondsSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.SECONDS.between(beginTime.localDateTime, endTime.localDateTime)

    val FULL = 0 //DATETIME
    val DATE = 1 //DATE
    val TIME = 2 //TIME
    val TIMESTAMP = 3 //TIME STAMP
}

class DateTime(private val dateTime: Any, private val formatStyle: String, private var mode: Int) {
    
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

    def this() {
        this("", "", DateTime.FULL)
    }

    def this(dateTime: Any) {
        this(dateTime, "", DateTime.FULL)
    }

    def this(dateTime: Any, formatStyle: String) {
        this(dateTime, formatStyle, DateTime.FULL)
    }

    def this(dateTime: Any, mode: Int) {
        this(dateTime, "", mode)
    }

    private val timezone: ZoneId = {
        var zone = ""
        breakable {
            for (z <- ZoneId.getAvailableZoneIds.asScala) {
                if (z.asInstanceOf[String].toLowerCase.contains(Global.TIMEZONE.toLowerCase)) {
                    zone = z
                    break
                }
            }
        }

        if (zone == "") {
            ZoneId.systemDefault()
        }
        else {
            ZoneId.of(zone)
        }
    }

    val localDateTime: LocalDateTime = {
        dateTime match {
            case localDateTime: LocalDateTime => localDateTime
            case str: String =>
                val dt = str.trim()
                if (dt == "") {
                    LocalDateTime.now(timezone)
                }
                else if ("^\\d+$".r.test(dt)) {
                    dt.length match {
                        case 10 | 13 | 17 => parseLocalDateTime(dt.toLong)
                        case _ => parseLocalDateTime(dt)
                    }
                }
                else {
                    parseLocalDateTime(dt, formatStyle)
                }
            case dt: DateTime => dt.localDateTime
            case date2: java.sql.Date =>
                mode = DateTime.DATE
                parseLocalDateTime(date2.toString + " 00:00:00", "yyyy-MM-dd HH:mm:ss")
            case time: java.sql.Time =>
                mode = DateTime.TIME
                parseLocalDateTime(LocalDate.now() + " " + time.toString, "yyyy-MM-dd HH:mm:ss")
            case timeStamp: java.sql.Timestamp =>
                mode = DateTime.FULL
                timeStamp.toLocalDateTime
            case date1: java.util.Date => date1.toInstant.atZone(timezone).toLocalDateTime
            case l: Long => parseLocalDateTime(l)
            case i: Int => parseLocalDateTime(i)
            case f: Float => parseLocalDateTime(f.toInt)
            case d: Double => parseLocalDateTime(d.toLong)
            case localDate: LocalDate => parseLocalDateTime(localDate.toString + " 00:00:00", "yyyy-MM-dd HH:mm:ss")
            case localTime: LocalTime => parseLocalDateTime(LocalDate.now() + " " + localTime.toString, "yyyy-MM-dd HH:mm:ss")
            case _ =>   throw new ConvertFailureException("Can't recognize as or convert to DateTime: " + dateTime)
        }
    }.atZone(timezone).toLocalDateTime

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
                LocalDateTime.parse( LocalDate.now(timezone).toString + " " + dateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd " + style))
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
                (timestamp, 0L)
            }
            //17位 纳秒
            else if (timestamp > 9999999999999L) {
                (timestamp / 10000000L, timestamp % 10000000L)
            }
            //13位 毫秒
            else {
                (timestamp / 1000L, timestamp % 1000L * 10000L)
            }
        }
        LocalDateTime.ofEpochSecond(second, nano.toInt, OffsetDateTime.now.getOffset)
    }

    def toDate: java.util.Date = java.util.Date.from(this.localDateTime.atZone(timezone).toInstant)
   
    def get(field: ChronoField): Int = this.localDateTime.get(field)
    def getYear: Int = this.localDateTime.getYear
    def getQuarter: Int = this.getString("Q").toInt
    def getQuarterName: String = this.getString("QQQ")
    def getFullQuarterName: String = this.getString("QQQQ")
    def getMonth: Int = this.localDateTime.getMonthValue
    def getMonthName: String = this.getString("MMM")
    def getFullMonthName: String = this.getString("MMMM")
    def getWeekName: String = this.getString("EEE").replace("星期", "周")
    def getFullWeekName: String = this.getString("EEEE")
    def getWeekOfMonth(firstDayOfWeek: DayOfWeek = DayOfWeek.MONDAY): Int = this.localDateTime.get(WeekFields.of(firstDayOfWeek, 1).weekOfMonth())
    def getWeekOfYear(firstDayOfWeek: DayOfWeek = DayOfWeek.MONDAY):Int = this.localDateTime.get(WeekFields.of(firstDayOfWeek, 1).weekOfYear())
    def getDayOfWeek: Int = this.localDateTime.getDayOfWeek.getValue
    def getDayOfMonth: Int = this.localDateTime.getDayOfMonth
    def getDayOfYear: Int = this.localDateTime.getDayOfYear
    def getHour: Int = this.localDateTime.getHour
    def getMinute: Int = this.localDateTime.getMinute
    def getSecond: Int = this.localDateTime.getSecond
    def getMilli: Int = this.localDateTime.getNano / 1000000
    def getMicro: Int = this.localDateTime.getNano / 1000
    def getNano: Int = this.localDateTime.getNano

    def getTickValue: String = this.getString("yyyy-MM-dd HH:mm:00")
    def getTockValue: String = this.getString("yyyy-MM-dd HH:00:00")

    def year: Int = this.localDateTime.getYear
    def quarter: Int = this.getQuarter
    def quarterName: String = this.getString("QQQ")
    def fullQuarterName: String = this.getString("QQQQ")
    def month: Int = this.localDateTime.getMonthValue
    def monthName: String = this.getString("MMM")
    def fullMonthName: String = this.getString("MMMM")
    def weekName: String = this.getString("EEE").replace("星期", "周")
    def fullWeekName: String = this.getString("EEEE")
    def weekOfMonth: Int = this.localDateTime.get(WeekFields.of(DayOfWeek.MONDAY, 1).weekOfMonth())
    def weekOfYear:Int = this.localDateTime.get(WeekFields.of(DayOfWeek.MONDAY, 1).weekOfYear())
    def dayOfWeek: Int = this.localDateTime.getDayOfWeek.getValue  //周一到周日分别为 1~7
    def dayOfMonth: Int = this.localDateTime.getDayOfMonth
    def dayOfYear: Int = this.localDateTime.getDayOfYear
    def hour: Int = this.localDateTime.getHour
    def minute: Int = this.localDateTime.getMinute
    def second: Int = this.localDateTime.getSecond
    def milli: Int = this.localDateTime.getNano / 1000000
    def micro: Int = this.localDateTime.getNano / 1000
    def nano: Int = this.localDateTime.getNano

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
    }

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
    def setMilli(value: Int): DateTime = {
        new DateTime(this.localDateTime.withNano(value * 1000000))
    }
    def setMicro(value: Int): DateTime = {
        new DateTime(this.localDateTime.withNano(value * 1000))
    }
    def setNano(value: Int): DateTime = {
        new DateTime(this.localDateTime.withNano(value))
    }
    def setBeginningOfMonth(): DateTime = {
        new DateTime(this.localDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0))
    }
    def setZeroOfDay(): DateTime = {
        new DateTime(this.localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0))
    }
   
    def toEpochSecond: Long = this.localDateTime.atZone(timezone).toInstant.getEpochSecond
    def toEpochMilli: Long = this.localDateTime.atZone(timezone).toInstant.toEpochMilli
    
    def plus(unit: ChronoUnit, amount: Int): DateTime = {
        new DateTime(this.localDateTime.plus(amount, unit))
    }
    def plus(unit: String, amount: Int): DateTime = {
        unit.toUpperCase() match {
            case "YEAR" => plusYears(amount)
            case "MONTH" => plusMonths(amount)
            case "DAY" => plusDays(amount)
            case "HOUR" => plusHours(amount)
            case "MINUTE" => plusMinutes(amount)
            case "SECOND" => plusSeconds(amount)
            case "MILLI" => plusMillis(amount)
            case "MICRO" => plusMicros(amount)
            case "NANO" => plusNanos(amount)
            case _ => this
        }
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
    def plusMillis(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusNanos(amount * 1000000))
    }
    def plusMicros(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusNanos(amount * 1000))
    }
    def plusNanos(amount: Long): DateTime = {
        new DateTime(this.localDateTime.plusNanos(amount))
    }
    
    def minus(unit: ChronoUnit, amount: Int): DateTime = {
        new DateTime(this.localDateTime.minus(amount, unit))
    }
    def minus(unit: String, amount: Int): DateTime = {
        unit.toUpperCase() match {
            case "YEAR" => minusYears(amount)
            case "MONTH" => minusMonths(amount)
            case "DAY" => minusDays(amount)
            case "HOUR" => minusHours(amount)
            case "MINUTE" => minusMinutes(amount)
            case "SECOND" => minusSeconds(amount)
            case "MILLI" => minusMillis(amount)
            case "MICRO" => minusMicros(amount)
            case "NANO" => minusNanos(amount)
            case _ => this
        }
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
    def minusMillis(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusNanos(amount * 1000000))
    }
    def minusMicros(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusNanos(amount * 1000))
    }
    def minusNanos(amount: Long): DateTime = {
        new DateTime(this.localDateTime.minusNanos(amount))
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

    def later(otherDateTime: DateTime): Long = this.toEpochMilli - otherDateTime.toEpochMilli
    def earlier(otherDateTime: DateTime): Long = otherDateTime.toEpochMilli - this.toEpochMilli
    def span(otherDateTime: DateTime): Long = Math.abs(this.toEpochMilli - otherDateTime.toEpochMilli)

    def copy(): DateTime = {
        new DateTime(this.localDateTime)
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
                case ("MILLI", Success(v)) => this.plusMillis(v)
                case ("MICRO", Success(v)) => this.plusMicros(v)
                case ("NANO", Success(v)) => this.plusNanos(v)
                case _ => this
            }
        }
        else {
            //=
            (field.toUpperCase(), Try(value.toInt)) match {
                case ("DAY", Success(v)) => if (v > 0) this.setDayOfMonth(v) else this.setDayOfMonth(1)
                case ("DAY", Failure(_)) =>  if (value.equalsIgnoreCase("L")) this.plusMonths(1).setDayOfMonth(1).plusDays(-1) else this.setDayOfWeek(value)  // L = last_day & WeekName = Mon,Tus...Sun
                case ("WEEK", Success(v)) => if (v < 0) this.setDayOfWeek(1) else if (v > 7) this.setDayOfWeek(7) else this.setDayOfWeek(v)
                case ("WEEK", Failure(_)) => this.setDayOfWeek(value)
                case ("MONTH", Success(v)) => this.setMonth(v)
                case ("YEAR", Success(v)) => this.setYear(v)
                case ("HOUR", Success(v)) => this.setHour(v)
                case ("MINUTE", Success(v)) => this.setMinute(v)
                case ("SECOND", Success(v)) => this.setSecond(v)
                case ("MILLI", Success(v)) => this.setMilli(v)
                case ("MICRO", Success(v)) => this.setMicro(v)
                case ("NANO", Success(v)) => this.setNano(v)
                case _ => this
            }
        }
    }

//    def express(row: DataRow): DateTime = {
//        var datatime = this
//
//        row.foreach((name, value) => {
//            if (name.contains("+") || name.contains("-")) {
//
//            }
//        })
//
//        datatime
//    }

    def express(expression: String): DateTime = {
        // Second|Minute|Hour|Day|Month|Year +|-|= num
        // exp includes num|Sun-Sat:1-7(only in day section)
        //year=2018#month=2#day=MON
        //month+1#day=1#day=MON#day-1
        var dateTime = this
        val sections = expression.toUpperCase().replaceAll("""\s""", "").replace("-", "=-").replace("+", "=+").split("#|&")
        for (section <- sections) {
            if (section.contains("=")) {
                //dateTime = this.shift(section.substring(0, section.indexOf("=")), section.substring(section.indexOf("=") + 1))
                dateTime = dateTime.shift(section.takeBefore("="), section.takeAfter("="))
            }
        }
        
        dateTime
    }
    
    def matches(chronExp: String): Boolean = ChronExp(chronExp).matches(this)
    
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
            val cronExp = CronExp(FILTER.toUpperCase())
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
        this.mode match {
            case DateTime.DATE => toDateString
            case DateTime.TIME => toTimeString
            case DateTime.TIMESTAMP => toEpochSecond.toString
            case _ => this.getString("yyyy-MM-dd HH:mm:ss")
        }
    }

    def toString(mode: String): String = {
        mode match {
            case "DATE" => toDateString
            case "TIME" => toTimeString
            case "TIMESTAMP" => toEpochSecond.toString
            case _ => this.getString("yyyy-MM-dd HH:mm:ss")
        }
    }

    def toDateString: String = {
        this.getString("yyyy-MM-dd")
    }

    def toTimeString: String = {
        this.getString("HH:mm:ss")
    }

    def toFullString: String = {
        this.getString("yyyy-MM-dd HH:mm:ss.SSS")
    }
}