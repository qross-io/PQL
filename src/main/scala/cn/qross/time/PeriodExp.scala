package cn.qross.time

import cn.qross.ext.TypeExt._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object PeriodExp {
    val MINUTELY: String = "MINUTELY"
    val HOURLY: String = "HOURLY"
    val DAILY: String = "DAILY"
    val WEEKLY: String = "WEEKLY"
    val MONTHLY: String = "MONTHLY"
    val YEARLY: String = "YEARLY"
    val ANNUAL: String = "ANNUAL"

    val BLANKS: mutable.LinkedHashMap[Regex, String] = mutable.LinkedHashMap[Regex, String](
        """\s\s""".r -> " ",
        """\s;""".r -> ";",
        """;\s""".r -> ";",
        """\s,""".r -> ",",
        """,\s""".r -> ",",
        """\s/""".r -> "/",
        """/\s""".r -> "/",
        """\s:""".r -> ":",
        """:\s""".r -> ":",
        """\s-""".r -> "-",
        """-\s""".r -> "-"
    )

    val RESERVED: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap[String, String](
        "MONDAY" -> "MON",
        "TUESDAY" -> "TUE",
        "WEDNESDAY" -> "WED",
        "THURSDAY" -> "THU",
        "FRIDAY" -> "FRI",
        "SATURDAY" -> "SAT",
        "SUNDAY" -> "SUN",
        "JANUARY" -> "JAN",
        "FEBRUARY" -> "FEB",
        "MARCH" -> "MAR",
        "APRIL" -> "APR",
        "JUNE" -> "JUN",
        "JULY" -> "JUL",
        "AUGUST" -> "AUG",
        "SEPTEMBER" -> "SEP",
        "OCTOBER" -> "OCT",
        "NOVEMBER" -> "NOV",
        "DECEMBER" -> "DEC",
        "FIRST-WORKDAY" -> "FW",
//        "SECOND-WORKDAY" -> "W2",
//        "THIRD-WORKDAY" -> "W3",
//        "FOURTH-WORKDAY" -> "W4",
//        "FIFTH-WORKDAY" -> "W5",
        "LAST-WORKDAY" -> "LW",
        "FIRST-HOLIDAY" -> "FR",
        //"SECOND-HOLIDAY" -> "R2",
        "LAST-HOLIDAY" -> "LR",
        "FIRST-DAY" -> "F",
        "LAST-DAY" -> "L",
        "WORKDAY" -> "W",
        "HOLIDAY" -> "R",
        //monthly
        "-OF-FIRST-WEEK" -> "#1",
        "-OF-SECOND-WEEK" -> "#2",
        "-OF-THIRD-WEEK" -> "#3",
        "-OF-FOURTH-WEEK" -> "#4",
        "-OF-FIFTH-WEEK" -> "#5",
        "-OF-SIXTH-WEEK" -> "#6",
        "-OF-LAST-WEEK" -> "#L",
        "FIRST-WEEK" -> "#1",
        "SECOND-WEEK" -> "#2",
        "THIRD-WEEK" -> "#3",
        "FOURTH-WEEK" -> "#4",
        "FIFTH-WEEK" -> "#5",
        "SIXTH-WEEK" -> "#6",
        "LAST-WEEK" -> "#L",
        //第几个周几，不支持
        "FIRST-" -> "F",
        "SECOND-" -> "2",
        "THIRD-" -> "3",
        "FOURTH-" -> "4",
        "FIFTH-" -> "5",
        "LAST-" -> "L",
        "-TO-" -> "-",
        "-AND-" -> "+"
    )
}

case class PeriodExp(expression: String) {

    //MINUTELY

    //HOURLY 4,10,25,44,20
    //HOURLY 0/5

    //DAILY 7-23/2:00
    //DAILY 12:35; 15:05

    //DAILY 0-3,13:00
    //DAILY 7,20:0/5
    //DAILY 7-20:0/5
    //DAILY 0/2:00

    //WEEKLY MON 12:35; FRI 12:30
    //WEEKLY MON,FRI 12:35; 8:30
    //WEEKLY MON 12:0/5

    //MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY
    //MON,TUE,WED,THU,FRI,SAT,SUN
    //WORKDAY,HOLIDAY
    //FIRST-WORKDAY,SECOND-WORKDAY,THIRD-WORKDAY,FOURTH-WORKDAY,FIFTH-WORKDAY,LAST-WORKDAY
    //FIRST-HOLIDAY,SECOND-HOLIDAY,LAST-HOLIDAY
    //LW,LR,W1,W2,W3,W4,W5,R1,R2

    //MONTHLY 01 12:30; 20 16:30; LAST-DAY 18:10; LAST-WORKDAY 17:00; HOLIDAY 12:00
    //MONTHLY MONDAY 12:45; FRIDAY 16:30
    //MONTHLY FIRST-MONDAY 12:45; FRIDAY 16:30
    //MONTHLY WORKDAY 12:45;HOLIDAY 16:30
    //MONTHLY WORKDAY,HOLIDAY 12:45; 16:30
    //MONTHLY WORKDAY,SUNDAY 12,16:0/5
    //MONTHLY FIRST-WEEK WORKDAY 12:45,30

    //WORKDAY-OF-FIRST-WEEK, WORKDAY-OF-LAST-WEEK = W#F, W#L
    //HOLIDAY-OF-FIRST-WEEK, HOLIDAY-OF-LAST-WEEK = R#F, R#L

    //YEARLY JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC

    //FIRST-WEEK,SECOND-WEEK,THIRD-WEEK,FOURTH-WEEK,FIFTH-WEEK,LAST-WEEK
    //MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY
    //FIRST-MONDAY,SECOND-MONDAY,THIRD-MONDAY,FOURTH-MONDAY,FIFTH-MONDAY,SIXTH-MONDAY,LAST-MONDAY
    //WORKDAY,HOLIDAY,LAST-DAY,LAST-WORKDAY,LAST-HOLIDAY

    //FIRST-MON,SECOND-MON,THIRD-MON,FOURTH-MON,FIFTH-MON,SIXTH-MON,LAST-MON

    //JANUARY,FEBRUARY,MARCH,APRIL,MAY,JUNE,JULY,AUGUST,SEPTEMBER,OCTOBER,NOVEMBER,DECEMBER


    //MONTHLY LAST-DAY 15:00
    //YEARLY AUG 15 15:00



    val trimmed: String = {
        var exp = expression.toUpperCase()
        //空白字符
        for ((tar, rep) <- PeriodExp.BLANKS) {
            while (tar.test(exp)) {
                exp = tar.replaceAllIn(exp, rep)
            }
        }
        //保留字
        for ((tar, rep) <- PeriodExp.RESERVED) {
            exp = exp.replace(tar, rep)
        }
        //第几周
//        """E(\d)-(MON|TUE|WED|THU|FRI|SAT|SUN|W|R)""".r.findAllMatchIn(exp)
//            .foreach(m => {
//                exp = exp.replace(m.group(0), m.group(2) + "#" + (if (m.group(1) == "7") "L" else m.group(1)))
//            })
//        """E(\d)""".r.findAllMatchIn(exp)
//            .foreach(m => {
//                exp = exp.replace(m.group(0), "#" + (if (m.group(1) == "7") "L" else m.group(1)))
//            })
        exp
    }

    val period: String = {
        ChronExp.PERIOD.findFirstMatchIn(trimmed) match {
            case Some(m) => m.group(1)
            case None =>
                if ("""^\d{4}""".r.test(trimmed) && trimmed.contains(" ")) {
                    trimmed.takeBefore(" ")
                }
                else {
                    throw new IllegalArgumentException("Empty or incorrect period type: " + expression + ", it must be MINUTELY/HOURLY/DAILY/WEEKLY/MONTHLY/YEARLY/ANNUAL or a specified year.")
                }
        }
    }

    private val group: Array[String] = trimmed.takeAfter(period).split("[;；]").map(_.trim())

    def toCron: List[CronExp] = {
        period match {
            case PeriodExp.MINUTELY => List[CronExp](CronExp())
            case PeriodExp.HOURLY =>
                group.map(s => {
                    CronExp(s"0 ${s.replaceAll("\\s", "")} * * * ? *")
                }).toList
            case PeriodExp.DAILY =>
                if (!group.head.contains(":")) {

                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val time = group(i)
                    if (time.contains(":")) {
                        list += CronExp(s"0 ${time.takeAfter(":")} ${time.takeBefore(":")} * * ? *")
                    }
                    else {
                        throw new IllegalArgumentException("Illegal time format, must be 'HH:mm', like '09:10'. " + time)
                    }
                }
                list.toList
            case PeriodExp.WEEKLY =>
                if (!group.head.contains(" ")) {
                    throw new IllegalArgumentException("Illegal date time format, must be 'DAY HH:mm', like 'MON 09:10'. " + group.head)
                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val formula = group(i)
                    val (week, time) = {
                        if (formula.contains(" ")) {
                            (formula.takeBefore(" "), formula.takeAfter(" "))
                        }
                        else {
                            (list(i-1).dayOfWeek, formula)
                        }
                    }

                    if (time.contains(":")) {
                        list += CronExp(s"0 ${time.takeAfter(":")} ${time.takeBefore(":")} ? * $week *")
                    }
                    else {
                        throw new IllegalArgumentException("Illegal time format, must be 'HH:mm', like '09:10'. " + time)
                    }
                }
                list.toList
            case PeriodExp.MONTHLY =>
                if (!group.head.contains(" ")) {
                    throw new IllegalArgumentException("Illegal date time format, must be 'DAY/WEEK HH:mm', like 'WORKDAY 09:10'. " + group.head)
                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val formula = group(i)
                    val (week, day, time) = {
                        if (formula.contains(" ")) {
                            (formula.takeBefore(" "), formula.takeBefore(" "), formula.takeAfter(" "))
                        }
                        else {
                            (list(i-1).dayOfWeek, list(i-1).dayOfMonth, formula)
                        }
                    }

                    if (time.contains(":")) {
                        val hour = time.takeBefore(":")
                        val minute = time.takeAfter(":")

                        if ("""(MON|TUE|WED|THU|FRI|SAT|SUN|#)""".r.test(formula)) {
                            list += CronExp(s"0 $minute $hour ? * $week *")
                        }
                        else {
                            list += CronExp(s"0 $minute $hour $day * ? *")
                        }
                    }
                    else {
                        throw new IllegalArgumentException("Illegal time format, must be 'HH:mm', like '09:10'. " + time)
                    }
                }
                list.toList
            case _ =>
                //YEARLY或指定年
                if (!group.head.contains(" ") || group.head.split(" ").length != 3) {
                    throw new IllegalArgumentException("Illegal date time format, must be 'MONTH DAY/WEEK HH:mm', such as 'JUN 01 09:10'. " + group.head)
                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val fields = group(i).split(" ")

                    val year = if (period == PeriodExp.YEARLY || period == PeriodExp.ANNUAL) "*" else period

                    val month = {
                        if (fields.length == 3) {
                            fields(0)
                        }
                        else {
                            list(i-1).month
                        }
                    }

                    val (week, day, time) = {
                        if (fields.length == 3) {
                            (fields(1), fields(1), fields(2))
                        }
                        else if (fields.length == 2) {
                            (fields(0), fields(0), fields(1))
                        }
                        else {
                            (list(i-1).dayOfWeek, list(i-1).dayOfMonth, fields(0))
                        }
                    }

                    if (time.contains(":")) {
                        val hour = time.takeBefore(":")
                        val minute = time.takeAfter(":")

                        if ("""(MON|TUE|WED|THU|FRI|SAT|SUN)""".r.test(group(i))) {
                            list += CronExp(s"0 $minute $hour ? $month $week $year")
                        }
                        else {
                            list += CronExp(s"0 $minute $hour $day $month ? $year")
                        }
                    }
                    else {
                        throw new IllegalArgumentException("Illegal time format, must be 'HH:mm', like '09:10'. " + time)
                    }
                }
                list.toList
        }
    }
}
