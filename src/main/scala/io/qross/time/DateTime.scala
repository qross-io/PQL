package io.qross.time

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{LocalDateTime, OffsetDateTime, ZoneId}
import java.util.regex.Pattern

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DateTime {
    
    def now: DateTime = DateTime()
    def from(dateTime: DateTime): DateTime = DateTime.now.setLocalDataTime(dateTime.localDateTime)
    def of(epochSecond: Long): DateTime = DateTime(epochSecond.toString, "EPOCH")
    def of(year: Int, month: Int, dayOfMonth: Int, hourOfDay: Int = 0, minute: Int = 0, second: Int = 0): DateTime = {
        DateTime(f"$year-$month%02d-$dayOfMonth%02d $hourOfDay%02d:$minute%02d:$second%02d", "yyyy-MM-dd HH:mm:ss")
    }
    
    def getDaysSpan(beginTime: String, endTime: String): Long = getDaysSpan(new DateTime(beginTime), new DateTime(endTime))
    def getDaysSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.DAYS.between(beginTime.localDateTime, endTime.localDateTime)
    def getSecondsSpan(beginTime: String, endTime: String): Long = getSecondsSpan(new DateTime(beginTime), new DateTime(endTime))
    def getSecondsSpan(beginTime: DateTime, endTime: DateTime): Long = ChronoUnit.SECONDS.between(beginTime.localDateTime, endTime.localDateTime)
}

case class DateTime(private var dateTime: String = "", private var formatStyle: String = "") {
    
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
    
    if (formatStyle == "") {
        formatStyle = dateTime.length match {
            case 8 => if (dateTime.contains(":")) {
                          dateTime = "1970-01-01 " + dateTime
                          "yyyy-MM-dd HH:mm:ss"
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
                else if (Pattern.compile("").matcher(dateTime).find()) {
                    "EPOCH"
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

    if (dateTime.length == 8) {
        dateTime += "00"
        formatStyle += "HH"
    }
    else if (dateTime.length == 10 && (dateTime.contains("-") || dateTime.contains("/"))) {
        dateTime += " 00"
        formatStyle += " HH"
    }

    private var localDateTime : LocalDateTime =
            if (formatStyle != "EPOCH") {
                dateTime match {
                    case "" => LocalDateTime.now()
                    case _ => LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern(formatStyle))
                }
            }
            else {
                Try(dateTime.toLong) match {
                    case Success(epoch) => LocalDateTime.ofEpochSecond(epoch, 0, OffsetDateTime.now.getOffset)
                    case Failure(_) => LocalDateTime.now()
                }
                
            }
   
    private def setLocalDataTime(localDateTime: LocalDateTime): DateTime = {
        this.localDateTime = localDateTime
        this
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
        this.localDateTime = this.localDateTime.`with`(field, value)
        this
    }
    def setYear(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withYear(value)
        this
    }
    def setMonth(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withMonth(value)
        this
    }
    def setDayOfMonth(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withDayOfMonth(value)
        this
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
            this.localDateTime = this.localDateTime.`with`(ChronoField.DAY_OF_WEEK, {
                value.substring(0, 2).toUpperCase() match {
                    case "MO" => 1
                    case "TU" => 2
                    case "WE" => 3
                    case "TH" => 4
                    case "FR" => 5
                    case "SA" => 6
                    case "SU" => 7
                    case _ => throw new IllegalArgumentException("Unknown Week name.")
                }
            })
        }
        else {
            throw new IllegalArgumentException("Week name must be 2 or more chars at least, please check.")
        }
        //while (value)
        this
    }
    def setDayOfWeek(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.`with`(ChronoField.DAY_OF_WEEK, value)
        this
    }
    def setHour(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withHour(value)
        this
    }
    def setMinute(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withMinute(value)
        this
    }
    def setSecond(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withSecond(value)
        this
    }
    def setNano(value: Int): DateTime = {
        this.localDateTime = this.localDateTime.withNano(value)
        this
    }
    
    def setZeroOfMonth(): DateTime = {
        this.localDateTime = this.localDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
        this
    }
    def setZeroOfDay(): DateTime = {
        this.localDateTime = this.localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0)
        this
    }
   
    def toEpochSecond: Long = this.localDateTime.atZone(ZoneId.systemDefault).toInstant.getEpochSecond
    
    def plus(unit: ChronoUnit, amount: Int): DateTime = {
        this.localDateTime = this.localDateTime.plus(amount, unit)
        this
    }
    def plusYears(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusYears(amount)
        this
    }
    def plusMonths(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusMonths(amount)
        this
    }
    def plusDays(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusDays(amount)
        this
    }
    def plusHours(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusHours(amount)
        this
    }
    def plusMinutes(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusMinutes(amount)
        this
    }
    def plusSeconds(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.plusSeconds(amount)
        this
    }
    
    def minus(unit: ChronoUnit, amount: Int): DateTime = {
        this.localDateTime = this.localDateTime.minus(amount, unit)
        this
    }
    def minusYears(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusYears(amount)
        this
    }
    def minusMonths(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusMonths(amount)
        this
    }
    def minusDays(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusDays(amount)
        this
    }
    def minusHours(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusHours(amount)
        this
    }
    def minusMinutes(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusMinutes(amount)
        this
    }
    def minusSeconds(amount: Long): DateTime = {
        this.localDateTime = this.localDateTime.minusSeconds(amount)
        this
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
        DateTime(this.getString("yyyy-MM-dd HH:mm:ss.SSS"))
    }
    
    def copy(dateTime: DateTime): Unit = {
        this.localDateTime = dateTime.localDateTime
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
                case _ =>
            }
        }
        else {
            //=
            (field.toUpperCase(), Try(value.toInt)) match {
                case ("DAY", Success(v)) => if (v > 0) this.setDayOfMonth(v)
                case ("DAY", Failure(_)) =>  if (value == "L") this.plusMonths(1).setDayOfMonth(1).plusDays(-1) else this.setDayOfWeek(value)  // L = last_day & WeekName = Mon,Tus...Sun
                case ("MONTH", Success(v)) => this.setMonth(v)
                case ("YEAR", Success(v)) => this.setYear(v)
                case ("HOUR", Success(v)) => this.setHour(v)
                case ("MINUTE", Success(v)) => this.setMinute(v)
                case ("SECOND", Success(v)) => this.setSecond(v)
                case _ =>
            }
        }
        
        this
    }
    
    def express(expression: String): DateTime = {
        // Second|Minute|Hour|Day|Month|Year +|-|= num
        // exp includes num|Sun-Sat:1-7(only in day section)
        //year=2018#month=2#day=MON
        //month+1#day=1#day=MON#day-1
        
        val sections = expression.toUpperCase().replace(" ", "").replace("-", "=-").replace("+", "=+").split("#")
        for (section <- sections) {
            if (section.contains("=")) {
                this.shift(section.substring(0, section.indexOf("=")), section.substring(section.indexOf("=") + 1))
            }
            else {
                """(?i)^(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)+""".r.findFirstIn(section) match {
                    case Some(f) => this.shift(f, section.substring(f.length))
                    case None =>
                }
            }
        }
        
        this
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
                            val field = item.substring(0, item.indexOf("="))
                            val value = item.substring(item.indexOf("=") + 1)
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