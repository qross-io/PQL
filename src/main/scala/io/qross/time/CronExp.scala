package io.qross.time

import java.time.temporal.{ChronoField, ChronoUnit}

import io.qross.ext.Output

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.{Success, Try}
import io.qross.ext.TypeExt._

object CronExp {

    val ASTERISK = "*"
    val QUESTION = "?"
    val COMMA = ","
    val MINUS = "-"
    val SLASH = "/"

    //以下关键字适用于dayOfMonth
    val FIRST_WORK_DAY = "FW"
    val FIRST_REST_DAY = "FR"
    val LAST_WORK_DAY = "LW"
    val LAST_REST_DAY = "LR"
    val FIRST = "F"
    val LAST = "L"
    val WORK_DAY = "W"
    val REST_DAY = "R"

    val HASH = "#"   //MON#2 dayOfWeek#week no.
    val SECOND = "SECOND"
    val MINUTE = "MINUTE"
    val HOUR = "HOUR"
    val DAY = "DAY"
    val MONTH = "MONTH"
    val WEEKDAY = "WEEKDAY"
    val WEEK = "WEEK"
    val YEAR = "YEAR"

    val WEEKS: Map[String, String]  = Map[String, String](
        "MON" -> "1",
        "TUE" -> "2",
        "WED" -> "3",
        "THU" -> "4",
        "FRI" -> "5",
        "SAT" -> "6",
        "SUN" -> "7"
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
        var tick = new DateTime(begin)
        val terminal = new DateTime(end)
        val ticks = new mutable.ListBuffer[String]()
        val exp = new CronExp(cronExp)
        while(tick.beforeOrEquals(terminal)) {
            exp.getNextTick(tick) match {
                case Some(nextTick) =>
                        if (nextTick.beforeOrEquals(terminal)) {
                            ticks += nextTick.getTickValue
                        }
                        if (nextTick.after(tick)) {
                            tick = new DateTime(nextTick)
                        }
                        tick = tick.plusMinutes(1)
                case None => tick = terminal.plusMinutes(1)  //future
            }
        }
        ticks.toList
    }

    implicit class Cron$String(var string: String) {
        //替换所有的月和星期关键字
        def replaceKeywords(): String = {
            for ((k, v) <- MONTHS) {
                string = string.replace(k, v)
            }

            for ((k, v) <- WEEKS) {
                string = string.replace(k, v)
            }

            string
        }
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
    DayOfMonth  , - * / ? F L W R 0-31
    Month , - * /  1-12 JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC
    DayOfWeek   , - * / ? F L W R # 1-7 MON,TUE,WED,THU,FRI,SAT,SUN
    Year  , - * / 1970-2100
    */

    private var nextTick: DateTime = _

    private val fields = {
        if (expression.contains("&") || expression.contains("=")) {
            val exp = mutable.LinkedHashMap[String, String](
                "SECOND" -> "0",
                "MINUTE" -> "*",
                "HOUR" -> "*",
                "DAY" -> "*",
                "MONTH" -> "*",
                "WEEK" -> "?",
                "YEAR" -> "*")
            expression
                    .replaceAll("\\s+", "")
                    .toUpperCase()
                    .replaceKeywords()
                    .split("&")
                    .foreach(item => {
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

            exp.values.toBuffer
        }
        else {
            expression.trim().toUpperCase().replaceKeywords().split("\\s+").toBuffer
        }
    }
    //check format
    if (fields.length < 1 || fields.length > 7) {
        throw new IllegalArgumentException("Incorrect corn expression format: " + expression)
    }
    else {
        if (fields.length < 7) {
            fields.+=:("0")
        }
        if (fields.length == 2) {
            fields += "*"
        }
        if (fields.length == 3) {
            fields += "*"
        }
        if (fields.length == 4) {
            fields += "*"
        }
        if (fields.length == 5) {
            fields += "?"
        }
        if (fields.length == 6) {
            fields += "*"
        }
    }

    val second: String = fields.head
    val minute: String = fields(1)
    val hour: String = fields(2)
    val dayOfMonth: String = {
        if (fields(3).contains(QUESTION) && fields(5).contains(QUESTION)) {
            ASTERISK
        }
        else {
            fields(3)
        }
    }
    val month: String = fields(4)
    val dayOfWeek: String = fields(5)
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
            case None => "N/A"
        }
    }
    def getNextTick(dateTime: String): Option[DateTime] = this.getNextTick(new DateTime(dateTime))
    def getNextTick(dateTime: DateTime): Option[DateTime] = {
        this.nextTick = dateTime.setNano(0)

        if (!this.second.contains(ASTERISK)) {
            tryMatch(SECOND)
        }

        //Output.writeMessage("AFTER SECOND " + nextTick)

        if (!this.minute.contains(ASTERISK)) {
            tryMatch(MINUTE)
        }

        //Output.writeMessage("AFTER MINUTE " + nextTick)

        if (!this.hour.contains(ASTERISK)) {
            tryMatch(HOUR)
        }

        //Output.writeMessage("AFTER HOUR " + this.nextTick)

        if (!this.dayOfMonth.contains(QUESTION) && !this.dayOfMonth.contains(ASTERISK)) {
            tryMatch(DAY)
        }
        else if (!this.dayOfWeek.contains(QUESTION) && !this.dayOfWeek.contains(ASTERISK)) {
            //L, week#n
            tryMatch(WEEK)
        }

        //Output.writeMessage("AFTER DAY AND WEEK " + this.nextTick)

        if (!this.month.contains(ASTERISK)) {
            tryMatch(MONTH)
        }

        //Output.writeMessage("AFTER MONTH " + this.nextTick)

        if (!this.year.contains(ASTERISK)) {
            tryMatch(YEAR)
        }

        //Output.writeMessage("AFTER YEAR " + this.nextTick)

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
        if (l < 1) l = 1

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
        val end = this.nextTick.plusMonths(1).setBeginningOfMonth().plusDays(-1).getDayOfMonth

        var value = this.dayOfMonth

        //处理关键字 FW,FR,LW,LR,F,L,W,R
        "FW|FR|LW|LR|F|L|W|R".r.findAllIn(value)
              .foreach {
                  case FIRST_WORK_DAY =>
                      value = value.replace(FIRST_WORK_DAY,
                          {
                              var date = this.nextTick.setBeginningOfMonth()
                              while (!WorkCalendar.isWorkday(date)) {
                                  date = date.plusDays(1)
                              }
                              date.getDayOfMonth.toString
                          }
                      )
                  case FIRST_REST_DAY =>
                      value = value.replace(FIRST_REST_DAY,
                          {
                              var date = this.nextTick.setBeginningOfMonth()
                              while (WorkCalendar.isWorkday(date)) {
                                  date = date.plusDays(1)
                              }
                              date.getDayOfMonth.toString
                          }
                      )
                  case LAST_WORK_DAY =>
                      value = value.replace(LAST_WORK_DAY,
                          {
                              var date = this.nextTick.plusMonths(1).setBeginningOfMonth().plusDays(-1)
                              while (!WorkCalendar.isWorkday(date)) {
                                  date = date.plusDays(-1)
                              }
                              date.getDayOfMonth.toString
                          }
                      )
                  case LAST_REST_DAY =>
                      value = value.replace(LAST_REST_DAY,
                          {
                              var date = this.nextTick.plusMonths(1).setBeginningOfMonth().plusDays(-1)
                              while (WorkCalendar.isWorkday(date)) {
                                  date = date.plusDays(-1)
                              }
                              date.getDayOfMonth.toString
                          }
                      )
                  case FIRST =>
                      value = value.replace(FIRST, "1")
                  case LAST =>
                      //倒数第几天
                      (1 to end).reverse.foreach(d => {
                          value = value.replace(d + LAST, (end - d + 1).toString)
                          value = value.replace(LAST + d, (end - d + 1).toString)
                      })
                      value = value.replace(LAST, end.toString)
                  case WORK_DAY =>
                      value = value.replace(WORK_DAY, this.getWorkDayOfMonth.mkString(","))
                  case REST_DAY =>
                      value = value.replace(REST_DAY, this.getRestDayOfMonth.mkString(","))
              }

        for (section <- value.split(COMMA)) {
            if (section.contains(MINUS) && section.contains(SLASH)) {
                parseMINUSAndSLASH(DAY, section, begin, end)
            }
            else if (section.contains(MINUS)) {
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

        everyMatch(WEEKDAY).clear()  //匹配到周几
        everyMatch(WEEK).clear()  //匹配到的日期

        // 1/2, 1-7, 2#3 第三周的周二, L1 最后一周的周一, 6L 最后一周的周六
        // FW, FR, LW, LR, W, R 表示每一周的第一个工作日、最后一个休息日等
        // FW#2, R#3 表示第二周的第一个工作日，第三周的所有工作日

        val weekDays = this.getWeekAndDayOfMonth

        val begin = 1
        val end = 7

        for (section <- this.dayOfWeek.split(COMMA)) {
            if (section.contains(HASH)) {
                //  m#n = week day OF week no.
                val week = section.takeBefore(HASH)
                val no = section.takeAfter(HASH)
                val days: mutable.TreeMap[Int, (Int, Boolean)] = {
                    if (no == "F") {
                        weekDays.head._2
                    }
                    else if (no == "L") {
                        weekDays.last._2
                    }
                    else {
                        var n = Try(no.toInt).getOrElse(1)
                        if (n < 1) n = 1
                        if (n > 6) n = 6
                        weekDays.getOrElse(n, new mutable.TreeMap[Int, (Int, Boolean)])
                    }
                }

                week.split("\\+")
                    .foreach(part => {
                        if (part == "") {
                            for (i <- 1 to 7) {
                                if (days.contains(i)) {
                                    everyMatch(WEEK) += days(i)._1
                                }
                            }
                        }
                        else if (part.contains("-")) {
                            val m = Try(part.takeBefore(MINUS).toInt).getOrElse(1)
                            val n = Try(part.takeAfter(MINUS).toInt).getOrElse(7)
                            for (i <- m to n) {
                                if (days.contains(i)) {
                                    everyMatch(WEEK) += days(i)._1
                                }
                            }
                        }
                        else if ("^\\d+$".r.test(part)) {
                            var m = part.toInt
                            if (m < begin) m = begin
                            if (m > end) m = end

                            if (days.contains(m)) {
                                everyMatch(WEEK) += days(m)._1
                            }
                        }
                        else {
                            //工作日休息日关键字
                            """^(FW|FR|LW|LR|W|R)$""".r.findFirstIn(week) match {
                                case Some(key) =>
                                    if (days.nonEmpty) {
                                        val work = new mutable.ArrayBuffer[Int]()
                                        val rest = new mutable.ArrayBuffer[Int]()
                                        breakable {
                                            for ((_, day) <- days) {
                                                if (day._2) {
                                                    //workday
                                                    work += day._1
                                                    if (key == "FW") {
                                                        break
                                                    }
                                                }
                                                else {
                                                    //holiday
                                                    rest += day._1
                                                    if (key == "FR") {
                                                        break
                                                    }
                                                }
                                            }
                                        }

                                        key match {
                                            case "FW" | "W" => everyMatch(WEEK) ++= work
                                            case "FR" | "R" => everyMatch(WEEK) ++= rest
                                            case "LW" => if (work.nonEmpty) everyMatch(WEEK) += work.last
                                            case "LR" => if (rest.nonEmpty) everyMatch(WEEK) += rest.last
                                        }
                                    }
                                case None => //不可识别的关键字
                            }
                        }
                    })
            }
            else if (section.contains(MINUS)) {
                parseMINUS(WEEKDAY, section, begin, end)
            }
            else if (section.contains(SLASH)) {
                parseSLASH(WEEKDAY, section, begin, end)
            }
            else {
                """^(FW|FR|LW|LR|W|R)$""".r.findFirstIn(section) match {
                    case Some(key) =>
                        for (weekday <- weekDays.values) {
                            val work = new mutable.ArrayBuffer[Int]()
                            val rest = new mutable.ArrayBuffer[Int]()
                            breakable {
                                for ((_, day) <- weekday) {
                                    if (day._2) {
                                        //workday
                                        work += day._1
                                        if (key == "FW") {
                                            break
                                        }
                                    }
                                    else {
                                        //holiday
                                        rest += day._1
                                        if (key == "FR") {
                                            break
                                        }
                                    }
                                }
                            }

                            key match {
                                case "FW" | "W" => everyMatch(WEEK) ++= work
                                case "FR" | "R" => everyMatch(WEEK) ++= rest
                                case "LW" => if (work.nonEmpty) everyMatch(WEEK) += work.last
                                case "LR" => if (rest.nonEmpty) everyMatch(WEEK) += rest.last
                            }
                        }
                    case None =>
                        if(section.contains(LAST)) {
                            val m = Try(section.replace(LAST, "").toInt).getOrElse(begin)
                            if (weekDays.last._2.contains(m)) {
                                //找最后一周
                                everyMatch(WEEK) += weekDays.last._2(m)._1
                            }
                            else {
                                //如果找不到去上一周找
                                breakable {
                                    for (i <- (1 to 5).reverse) {
                                        if (weekDays(i).contains(m)) {
                                            everyMatch(WEEK) += weekDays(i)(m)._1
                                            break
                                        }
                                    }
                                }
                            }
                        }
                        else if (section.contains(FIRST)) {
                            val m = Try(section.replace(FIRST, "").toInt).getOrElse(begin)
                            breakable {
                                for (i <- 1 to 6) {
                                    if (weekDays(i).contains(m)) {
                                        everyMatch(WEEK) += weekDays(i)(m)._1
                                        break
                                    }
                                }
                            }
                        }
                        else if ("^\\d{2}$".r.test(section)) {
                            var no = section.take(1).toInt
                            var week = section.drop(1).toInt

                            if (no > 5) {
                                no = 5
                            }
                            if (week > 7) {
                                week = 7
                            }

                            var m = 0
                            breakable {
                                for (i <- 1 to 6) {
                                    if (weekDays(i).contains(week)) {
                                        m += 1
                                        if (m == no) {
                                            everyMatch(WEEK) += weekDays(i)(week)._1
                                            break
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            parseSingleValue(WEEKDAY, section, begin, end)
                        }
                }
            }
        }

        //用周查找对应的日期
        if (everyMatch(WEEKDAY).nonEmpty) {
            for (weekDay <- weekDays.values) {
                for ((week, day) <- weekDay) {
                    if (everyMatch(WEEKDAY).contains(week)) {
                        everyMatch(WEEK) += day._1
                    }
                }
            }
        }
    }

    private def resetPreviousMatchFrom(chronoName: String): Unit = {
        chronoName match  {
            case YEAR =>
                resetMatchFrom(MONTH)
            case MONTH =>
                resetMatchFrom(DAY)
            case DAY | WEEK =>
                    resetMatchFrom(HOUR)
            case HOUR =>
                resetMatchFrom(MINUTE)
            case MINUTE =>
                resetMatchFrom(SECOND)
            case SECOND => //没有上一级, 所以不可能有这种情况
            case _ =>

        }
    }
    private def resetMatchFrom(chronoName: String): Unit = {
        chronoName match  {
            case YEAR => //不可能有这种情况
            case MONTH =>
                    this.nextTick = this.nextTick.set(ChronoField.MONTH_OF_YEAR, if (this.month.contains(ASTERISK)) 1 else this.everyMatch(MONTH).head)
                    resetMatchFrom(DAY)
            case DAY | WEEK =>
                    if (!this.dayOfMonth.contains(QUESTION)) {
                        parseDAY()
                        this.nextTick = this.nextTick.set(ChronoField.DAY_OF_MONTH, if (this.dayOfMonth.contains(ASTERISK)) 1 else this.everyMatch(DAY).head)
                        resetMatchFrom(HOUR)
                    }
                    else {
                        parseWEEK()
                        //直到找到有匹配值的月为止
                        while (everyMatch(WEEK).isEmpty) {
                            this.nextTick = this.nextTick.plus(ChronoUnit.MONTHS, 1).set(ChronoField.DAY_OF_MONTH, 1)
                            parseWEEK()
                        }
                        this.nextTick = this.nextTick.set(ChronoField.DAY_OF_MONTH, if (this.dayOfWeek.contains(ASTERISK)) 1 else this.everyMatch(WEEK).head)
                        resetMatchFrom(HOUR)
                    }
            case HOUR =>
                    this.nextTick = this.nextTick.set(ChronoField.HOUR_OF_DAY, if (this.hour.contains(ASTERISK)) 0 else this.everyMatch(HOUR).head)
                    resetMatchFrom(MINUTE)
            case MINUTE =>
                    this.nextTick = this.nextTick.set(ChronoField.MINUTE_OF_HOUR, if (this.minute.contains(ASTERISK)) 0 else this.everyMatch(MINUTE).head)
                    resetMatchFrom(SECOND)
            case SECOND =>
                    this.nextTick = this.nextTick.set(ChronoField.SECOND_OF_MINUTE, if (this.second.contains(ASTERISK)) 0 else this.everyMatch(SECOND).head)
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

        if (everyMatch(chronoName).nonEmpty) {
            //var matched = everyMatch(chronoName).isEmpty //skip match if empty
            var matched = false
            while (!matched && this.nextTick != null) {
                val matchValue = this.nextTick.get(chronoField)

                //not found
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
                            this.nextTick = this.nextTick.plus(nextChronoUnit, 1).set(chronoField, begin)
                            resetMatchFrom(chronoName)
                        }
                        else {
                            this.nextTick = null
                        }
                    }
                    //found
                    else {
                        matched = true
                        this.nextTick = this.nextTick.set(chronoField, next)
                        resetPreviousMatchFrom(chronoName)
                    }
                }
                //found
                else {
                    matched = true
                }
            }
        }
        else {
            //有可能找不到任何匹配的情况，比如最后一个周五
            if (chronoName != YEAR) {
                this.nextTick = this.nextTick.plus(nextChronoUnit, 1).set(chronoField, begin)
                resetMatchFrom(chronoName)
            }
            else {
                this.nextTick = null
            }
        }
    }
    
    private def getWorkDayOfMonth: List[Int] = {
        var day = this.nextTick.setBeginningOfMonth()
        val lastDay = day.plusMonths(1).plusDays(-1)
        
        var list = new ArrayBuffer[Int]
        while (day.beforeOrEquals(lastDay)) {
            if (WorkCalendar.isWorkday(day)) {
                list += day.getDayOfMonth
            }
            day = day.plusDays(1)
        }
        
        list.toList
    }

    private def getRestDayOfMonth: List[Int] = {
        var day = this.nextTick.setBeginningOfMonth()
        val lastDay = day.plusMonths(1).plusDays(-1)

        var list = new ArrayBuffer[Int]
        while (day.beforeOrEquals(lastDay)) {
            if (!WorkCalendar.isWorkday(day)) {
                list += day.getDayOfMonth
            }
            day = day.plusDays(1)
        }

        list.toList
    }

    // week no. -> day of week -> day of month + workday
    // for cron
    private def getWeekAndDayOfMonth: mutable.TreeMap[Int, mutable.TreeMap[Int, (Int, Boolean)]] = {
        val map = new mutable.TreeMap[Int, mutable.TreeMap[Int, (Int, Boolean)]]()
        var no = 1 //week no.

        val end = this.nextTick.copy().plusMonths(1).setDayOfMonth(1).minusDays(1)
        for (d <- 1 to end.getDayOfMonth) {
            val date = end.setDayOfMonth(d)
            val week = date.getDayOfWeek //.getString("e").toInt
            val day = date.getDayOfMonth
            if (day > 1 && week == 1) {
                no += 1
            }

            if (!map.contains(no)) {
                map += (no -> new mutable.TreeMap[Int, (Int, Boolean)]())
            }
            map(no) += (week -> (day, WorkCalendar.isWorkday(date)))
        }
        
        map
    }
}