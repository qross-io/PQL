package io.qross.time

import java.time.temporal.{ChronoField, ChronoUnit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.{Success, Try}

object CronExp {

    val ASTERISK = "*"
    val QUESTION = "?"
    val COMMA = ","
    val MINUS = "-"
    val SLASH = "/"
    val LAST_DAY = "L"
    val WORK_DAY = "W"
    val LAST_WORK_DAY = "LW"
    val HASH = "#"   //MON#2 dayOfWeek
    val SECOND = "SECOND"
    val MINUTE = "MINUTE"
    val HOUR = "HOUR"
    val DAY = "DAY"
    val MONTH = "MONTH"
    val WEEKDAY = "WEEKDAY"
    val WEEK = "WEEK"
    val YEAR = "YEAR"
    val WEEKS: Map[String, String]  = Map[String, String](
        "SUN" -> "1",
        "MON" -> "2",
        "TUE" -> "3",
        "WED" -> "4",
        "THU" -> "5",
        "FRI" -> "6",
        "SAT" -> "7"
    )
    val MONTHS: Map[String, String] = Map[String, String](
        "JAN" -> "1",
        "FEB" -> "2",
        "MAR" -> "3",
        "APR" -> "4",
        "MAY" -> "5",
        "JUN" -> "6",
        "JUL" -> "7",
        "AUG" -> "8",
        "SEP" -> "9",
        "OCT" -> "10",
        "NOV" -> "11",
        "DEC" -> "12"
    )

    def parse(expression: String = "0 * * * * ? *") = new CronExp(expression)

    def getTicks(cronExp: String, begin: String, end: String): List[String] = {
        var tick = DateTime(begin)
        val terminal = DateTime(end)
        val ticks = new mutable.ListBuffer[String]()
        val exp = new CronExp(cronExp)
        while(tick.beforeOrEquals(terminal)) {
            exp.getNextTick(tick) match {
                case Some(nextTick) =>
                        if (nextTick.beforeOrEquals(terminal)) {
                            ticks += nextTick.getTickValue
                        }
                        if (nextTick.after(tick)) {
                            tick.copy(nextTick)
                        }
                        tick.plusMinutes(1)
                case None => tick = terminal.plusMinutes(1)  //future
            }
        }
        ticks.toList
    }
}

case class CronExp(expression: String = "0 * * * * ? *") {

    import io.qross.time.CronExp._

    /*
    second, minute, hour, dayOfMonth, month, dayOfWeek, year
    0 * * * * * *

    Second  , - * /  0-59
    Minute , - * /  0-59
    Hour   , - * /  0-23
    DayOfMonth  , - * / ? L W 0-31
    Month , - * /  1-12 JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC
    DayOfWeek   , - * / ? L # 1-7 SUN,MON,TUE,WED,THU,FRI,SAT
    Year  , - * / 1970-2099
    */
    
    private var nextTick: DateTime = _

    private val fields = expression.toUpperCase().split(" ")
    if (fields.length != 7) throw new IllegalArgumentException("expression must has 7 fields")
    
    private val second: String = fields(0)
    private val minute: String = fields(1)
    private val hour: String = fields(2)
    private val dayOfMonth: String = {
        if (fields(3).contains(QUESTION) && fields(5).contains(QUESTION)) {
            ASTERISK
        }
        else {
            fields(3)
        }
    }
    private val month: String = {
        var value = fields(4)
        for ((k, v) <- MONTHS) {
            value = value.replace(k, v)
        }
        value
    }
    private val dayOfWeek: String = {
        var value = fields(5)
        for ((k, v) <- WEEKS) {
            value = value.replace(k, v)
        }
        value
    }
    private val year: String = fields(6)
    
    private val everyMatch = mutable.HashMap[String, mutable.TreeSet[Int]](
        SECOND -> new mutable.TreeSet[Int](),
        MINUTE -> new mutable.TreeSet[Int](),
        HOUR -> new mutable.TreeSet[Int](),
        DAY -> new mutable.TreeSet[Int](),
        MONTH -> new mutable.TreeSet[Int](),
        WEEK -> new mutable.TreeSet[Int](),
        WEEKDAY -> new mutable.TreeSet[Int](),
        YEAR -> new mutable.TreeSet[Int]()
    )
    
    //if matches with a DateTime
    def matches(dateTime: DateTime): Boolean = {
        this.nextTick = dateTime
        var result = true
        
        if (!this.second.contains(ASTERISK)) {
            result = isMatch(SECOND)
        }

        if (result && !this.minute.contains(ASTERISK)) {
            result = isMatch(MINUTE)
        }
        
        if (result && !this.hour.contains(ASTERISK)) {
            result = isMatch(HOUR)
        }
    
        if (result) {
            if (!this.dayOfMonth.contains(QUESTION) && !this.dayOfMonth.contains(ASTERISK)) {
                result = isMatch(DAY)
            }
            else if (!this.dayOfWeek.contains(QUESTION) && !this.dayOfWeek.contains(ASTERISK)) {
                result = isMatch(WEEK)
            }
        }
    
        if (result && !this.month.contains(ASTERISK)) {
            result = isMatch(MONTH)
        }
    
        if (result && !this.year.contains(ASTERISK)) {
            result = isMatch(YEAR)
        }
        
        result
    }
    
    //find next tick
    def getNextTickOrNone(dateTime: DateTime): String = {
        getNextTick(dateTime) match {
            case Some(tick) => tick.getTickValue
            case None => "NONE"
        }
    }
    def getNextTick(dateTime: String): Option[DateTime] = this.getNextTick(DateTime(dateTime))
    def getNextTick(dateTime: DateTime): Option[DateTime] = {
        this.nextTick = dateTime.copy()
   
        if (!this.second.contains(ASTERISK)) {
            tryMatch(SECOND)
        }
        
        //writeMessage("AFTER SECOND " + nextTick)
        
        if (!this.minute.contains(ASTERISK)) {
            tryMatch(MINUTE)
        }
    
        //writeMessage("AFTER MINUTE " + nextTick)
        
        if (!this.hour.contains(ASTERISK)) {
            tryMatch(HOUR)
        }
    
        //writeMessage("AFTER HOUR " + this.nextTick)
        
        if (!this.dayOfMonth.contains(QUESTION) && !this.dayOfMonth.contains(ASTERISK)) {
            tryMatch(DAY)
        }
        else if (!this.dayOfWeek.contains(QUESTION) && !this.dayOfWeek.contains(ASTERISK)) {
            //L, week#n
            tryMatch(WEEK)
        }

        //writeMessage("AFTER DAY AND WEEK " + this.nextTick)
        
        if (!this.month.contains(ASTERISK)) {
            tryMatch(MONTH)
        }
    
        //writeMessage("AFTER MONTH " + this.nextTick)
        
        if (!this.year.contains(ASTERISK)) {
            tryMatch(YEAR)
        }
    
        //writeMessage("AFTER YEAR " + this.nextTick)
    
        Option(this.nextTick)
    }

    private def parseMINUSAndSLASH(chronoName: String, section: String, begin: Int, end: Int): Unit = {
        var m = Try(section.substring(0, section.indexOf(MINUS)).toInt).getOrElse(begin)
        var n = Try(section.substring(section.indexOf(MINUS) + 1, section.lastIndexOf(SLASH)).toInt).getOrElse(end)
        var l = Try(section.substring(section.lastIndexOf(SLASH) + 1).toInt).getOrElse(2)

        if (m < begin) m = begin
        if (m > end) m = end
        if (n < begin) n = begin
        if (n > end) n = end
        if (l < 2) l = 2

        while (m <= n) {
            everyMatch(chronoName) += m
            m += l
        }
    }
    
    private def parseMINUS(chronoName: String, section: String, begin: Int, end: Int): Unit = {
        var m = Try(section.substring(0, section.indexOf(MINUS)).toInt).getOrElse(begin)
        var n = Try(section.substring(section.indexOf(MINUS) + 1).toInt).getOrElse(end)
    
        if (m < begin) m = begin
        if (m > end) m = end
        if (n < begin) n = begin
        if (n > end) n = end
 
        if (m < n) {
            (m to n).foreach(everyMatch(chronoName) += _)
        }
        else if (m > n) {
            (n to m).foreach(everyMatch(chronoName) += _)
        }
        else {
            everyMatch(chronoName) += m
        }
    }
    
    private def parseSLASH(chronoName: String, section: String, begin: Int, end: Int): Unit = {
        var m = Try(section.substring(0, section.indexOf(SLASH)).toInt).getOrElse(begin) //start point
        var n = Try(section.substring(section.indexOf(SLASH) + 1).toInt).getOrElse(1) //step length
    
        if (m < begin) m = begin
        if (m > end) m = end
        if (n < 1) n = 1
        //if (n > end / 2) n = end / 2
    
        while (m <= end) {
            everyMatch(chronoName) += m
            m += n
        }
    }
    
    private def parseSingleValue(chronoName: String, section: String, begin: Int, end: Int): Unit = {
        Try(section.toInt) match {
            case Success(v) => everyMatch(chronoName) += {
                                    if (v < begin) {
                                        begin
                                    }
                                    else if (v > end) {
                                        end
                                    }
                                    else {
                                        v
                                    }
                                }
            case _ =>
        }
    }
    
    //parse fields excluding DAY and WEEK
    private def parseCommon(chronoName:String, value: String, begin: Int, end: Int): Unit = {
        //clear the field data first
        everyMatch(chronoName).clear()
       
        val sections = value.split(COMMA)
        for (section <- sections) {
            if (section.contains(MINUS) && section.contains(SLASH)) {
                parseMINUSAndSLASH(chronoName, section, begin, end)
            }
            else if (section.contains(MINUS)) {
                parseMINUS(chronoName, section, begin, end)
            }
            else if (section.contains(SLASH)) {
                parseSLASH(chronoName, section, begin, end)
            }
            else {
                parseSingleValue(chronoName, section, begin, end)
            }
        }
    }
    
    private def parseDAY(): Unit = {
        everyMatch(DAY).clear()
        
        val begin = 1
        val end = this.nextTick.copy().plusMonths(1).setZeroOfMonth().plusDays(-1).getDayOfMonth
      
        var value = this.dayOfMonth
        //LW,L,W
        if (value.contains(LAST_WORK_DAY)) {
            value = value.replace(LAST_WORK_DAY,
                {
                    val date = this.nextTick.copy().plusMonths(1).setZeroOfMonth().plusDays(-1)
                    while (date.getWeekName == "Sat" || date.getWeekName == "Sun") {
                        date.plusDays(-1)
                    }
                    date.getDayOfMonth.toString
                }
            )
        }
        if (value.contains(LAST_DAY)) {
            (1 to 31).reverse.foreach(d => {
                value = value.replace(d + LAST_DAY, (end - d + 1).toString)
                value = value.replace(LAST_DAY + d, (end - d + 1).toString)
            })
            value = value.replace(LAST_DAY, end.toString)
        }
        if (value.contains(WORK_DAY)) {
            value = value.replace(WORK_DAY, this.getWorkDayOfMonth.mkString(","))
        }
        
        val sections = value.split(COMMA)
        for (section <- sections) {
            if (section.contains(MINUS)) {
                parseMINUS(DAY, section, begin, end)
            }
            else if (section.contains(SLASH)) {
                parseSLASH(DAY, section, begin, end)
            }
            else {
                parseSingleValue(DAY, section, begin, end)
            }
        }
    }
    
    private def parseWEEK(): Unit = {
    
        everyMatch(WEEKDAY).clear()
        everyMatch(WEEK).clear()
        
        // 1/2, 1-7, 2#3, L1, 6L
        val weekDays = this.getWeekAndDayOfMonth
        
        val begin = 1
        val end = 7
        
        val sections = this.dayOfWeek.split(COMMA)
        for (section <- sections) {
            if (section.contains(MINUS)) {
                parseMINUS(WEEKDAY, section, begin, end)
            }
            else if (section.contains(SLASH)) {
                parseSLASH(WEEKDAY, section, begin, end)
            }
            else if (section.contains(HASH)) {
                //  m#n = week day OF week no.
                var m = Try(section.substring(0, section.indexOf(HASH)).toInt).getOrElse(begin)
                var n = Try(section.substring(section.indexOf(HASH) + 1).toInt).getOrElse(1)
                
                if (m < begin) m = begin
                if (m > end) m = end
                if (n < 1) n = 1
                if (n > 6) n = 6
                
                if (weekDays.contains(n)) {
                    if (weekDays(n).contains(m)) {
                        everyMatch(WEEK) += weekDays(n)(m)
                    }
                }
            }
            else if(section.contains(LAST_DAY)) {
                val m = Try(section.replace("L", "").toInt).getOrElse(begin)
                if (weekDays.last._2.contains(m)) {
                    everyMatch(WEEK) += weekDays.last._2(m)
                }
            }
            else {
                parseSingleValue(WEEKDAY, section, begin, end)
            }
        }
        
        for (weekDay <- weekDays.values) {
            for ((week, day) <- weekDay) {
                if (everyMatch(WEEKDAY).contains(week)) {
                    everyMatch(WEEK) += day
                }
            }
        }
    }
    
    private def resetMatchFrom(chronoName: String): Unit = {
        chronoName match  {
            case YEAR =>
            case MONTH =>
                    this.nextTick.set(ChronoField.MONTH_OF_YEAR, if (this.month.contains(ASTERISK)) 1 else this.everyMatch(MONTH).head)
                    resetMatchFrom(DAY)
            case DAY | WEEK =>
                    if (!this.dayOfMonth.contains(QUESTION)) {
                        parseDAY()
                        this.nextTick.set(ChronoField.DAY_OF_MONTH, if (this.dayOfMonth.contains(ASTERISK)) 1 else this.everyMatch(DAY).head)
                        resetMatchFrom(HOUR)
                    }
                    else {
                        parseWEEK()
                        this.nextTick.set(ChronoField.DAY_OF_MONTH, if (this.dayOfWeek.contains(ASTERISK)) 1 else this.everyMatch(WEEK).head)
                        resetMatchFrom(HOUR)
                    }
            case HOUR =>
                    this.nextTick.set(ChronoField.HOUR_OF_DAY, if (this.hour.contains(ASTERISK)) 0 else this.everyMatch(HOUR).head)
                    resetMatchFrom(MINUTE)
            case MINUTE =>
                    this.nextTick.set(ChronoField.MINUTE_OF_HOUR, if (this.minute.contains(ASTERISK)) 0 else this.everyMatch(MINUTE).head)
                    resetMatchFrom(SECOND)
            case SECOND =>
                    this.nextTick.set(ChronoField.SECOND_OF_MINUTE, if (this.second.contains(ASTERISK)) 0 else this.everyMatch(SECOND).head)
            case _ =>

        }
    }
    
    private def isMatch(chronoName: String): Boolean = {
        val (value, begin, end, chronoField, nextChronoUnit) = chronoName match {
            case SECOND => (this.second, 0, 59, ChronoField.SECOND_OF_MINUTE, ChronoUnit.MINUTES)
            case MINUTE => (this.minute, 0, 59, ChronoField.MINUTE_OF_HOUR, ChronoUnit.HOURS)
            case HOUR => (this.hour, 0, 23, ChronoField.HOUR_OF_DAY, ChronoUnit.DAYS)
            case DAY => (this.dayOfMonth, 1, 31, ChronoField.DAY_OF_MONTH, ChronoUnit.MONTHS)
            case MONTH => (this.month, 1, 12, ChronoField.MONTH_OF_YEAR, ChronoUnit.YEARS)
            case WEEK => (this.dayOfWeek, 1, 7, ChronoField.DAY_OF_MONTH, ChronoUnit.MONTHS)
            case YEAR => (this.year, 1970, 2099, ChronoField.YEAR, ChronoUnit.CENTURIES)
        }
    
        if (chronoName != WEEK) {
            if (chronoName != DAY) {
                parseCommon(chronoName, value, begin, end)
            }
            else {
                parseDAY()
            }
        }
        else {
            parseWEEK()
        }
    
        everyMatch(chronoName).isEmpty || everyMatch(chronoName).contains(this.nextTick.get(chronoField))
    }
    
    private def tryMatch(chronoName: String): Unit = {
        val (value, begin, end, chronoField, nextChronoUnit) = chronoName match {
            case SECOND => (this.second, 0, 59, ChronoField.SECOND_OF_MINUTE, ChronoUnit.MINUTES)
            case MINUTE => (this.minute, 0, 59, ChronoField.MINUTE_OF_HOUR, ChronoUnit.HOURS)
            case HOUR => (this.hour, 0, 23, ChronoField.HOUR_OF_DAY, ChronoUnit.DAYS)
            case DAY => (this.dayOfMonth, 1, 31, ChronoField.DAY_OF_MONTH, ChronoUnit.MONTHS)
            case MONTH => (this.month, 1, 12, ChronoField.MONTH_OF_YEAR, ChronoUnit.YEARS)
            case WEEK => (this.dayOfWeek, 1, 7, ChronoField.DAY_OF_MONTH, ChronoUnit.MONTHS)
            case YEAR => (this.year, 1970, 2099, ChronoField.YEAR, ChronoUnit.CENTURIES)
        }
    
        if (chronoName != WEEK) {
            if (chronoName != DAY) {
                parseCommon(chronoName, value, begin, end)
            }
            else {
                parseDAY()
            }
        }
        else {
            parseWEEK()
        }
        
        var matched = everyMatch(chronoName).isEmpty //skip match if empty
        while (!matched && this.nextTick != null) {
            val matchValue = this.nextTick.get(chronoField)

            //note found
            if (!everyMatch(chronoName).contains(matchValue)) {

                var next = matchValue
                breakable {
                    for (v <- everyMatch(chronoName)) {
                        if (v > next && next == matchValue) {
                            next = v
                            break
                        }
                    }
                }

                //not found
                if (next == matchValue) {
                    if (chronoName != YEAR) {
                        this.nextTick.plus(nextChronoUnit, 1).set(chronoField, begin)
                        resetMatchFrom(chronoName)
                    }
                    else {
                        this.nextTick = null
                    }
                }
                //found
                else {
                    matched = true
                    this.nextTick.set(chronoField, next)
                    //resetMatchFrom(chronoName)
                }
            }
            //found
            else {
                matched = true
            }
        }
    }
    
    private def getWorkDayOfMonth: List[Int] = {
        val day = this.nextTick.copy().setZeroOfMonth()
        val lastDay = day.copy().plusMonths(1).plusDays(-1)
        
        var list = new ArrayBuffer[Int]
        while (day.beforeOrEquals(lastDay)) {
            if (day.getWeekName != "Sat" && day.getWeekName != "Sun") {
                list += day.getDayOfMonth
            }
            day.plusDays(1)
        }
        
        list.toList
    }
    // week no. -> dates
    // for cron
    private def getWeekAndDayOfMonth: mutable.TreeMap[Int, mutable.TreeMap[Int, Int]] = {
        var map = new mutable.TreeMap[Int, mutable.TreeMap[Int, Int]]()
        var no = 1
        val date = this.nextTick.copy().plusMonths(1).setDayOfMonth(1).plusDays(-1)
        for (day <- 1 to date.getDayOfMonth) {
            val week: Int = date.setDayOfMonth(day).getString("e").toInt
            if (day > 1 && week == 1) {
                no += 1
            }
            
            if (!map.contains(no)) {
                map += (no -> new mutable.TreeMap[Int, Int]())
            }
            map(no) += (week -> day)
        }
        
        map
    }
}