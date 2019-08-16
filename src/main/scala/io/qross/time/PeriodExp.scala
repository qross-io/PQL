package io.qross.time

import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object PeriodExp {
    val MINUTELY: String = "MINUTELY"
    val HOURLY: String = "HOURLY"
    val DAILY: String = "DAILY"
    val WEEKLY: String = "WEEKLY"
    val MONTHLY: String = "MONTHLY"
    val YEARLY: String = "*"
    val ANNUAL: String = "*"

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
        "FIRST-WORK-DAY" -> "W1",
        "SECOND-WORK-DAY" -> "W2",
        "THIRD-WORK-DAY" -> "W3",
        "FOURTH-WORK-DAY" -> "W4",
        "FIFTH-WORK-DAY" -> "W5",
        "LAST-WORK-DAY" -> "LW",
        "FIRST-REST-DAY" -> "R1",
        "SECOND-REST-DAY" -> "R2",
        "LAST-REST-DAY" -> "LR",
        "LAST-DAY" -> "L",
        "WORK-DAY" -> "W",
        "REST-DAY" -> "R",
        //monthly
        "FIRST-WEEK-" -> "E1",
        "SECOND-WEEK-" -> "E2",
        "THIRD-WEEK-" -> "E3",
        "FOURTH-WEEK-" -> "E4",
        "FIFTH-WEEK-" -> "E5",
        "SIXTH-WEEK-" -> "E6",
        "LAST-WEEK-" -> "LE",
        "FIRST-" -> "E1",
        "SECOND-" -> "E2",
        "THIRD-" -> "E3",
        "FOURTH-" -> "E4",
        "FIFTH-" -> "E5",
        "SIXTH-" -> "E6",
        "LAST-" -> "LE",
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
        "DECEMBER" -> "DEC"
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

    //WEEKLY MON 12:35; FRI 12:30
    //WEEKLY MON,FRI 12:35; 8:30
    //WEEKLY MON 12:0/5

    //MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY
    //MON,TUE,WED,THU,FRI,SAT,SUN
    //WORK-DAY,REST-DAY
    //FIRST-WORK-DAY,SECOND-WORK-DAY,THIRD-WORK-DAY,FOURTH-WORK-DAY,FIFTH-WORK-DAY,LAST-WORK-DAY
    //FIRST-REST-DAY,SECOND-REST-DAY,LAST-REST-DAY
    //LW,LR,W1,W2,W3,W4,W5,R1,R2


    //MONTHLY 01 12:30; 20 16:30; LAST-DAY 18:10; LAST-WORK-DAY 17:00; REST-DAY 12:00
    //MONTHLY MONDAY 12:45; FRIDAY 16:30
    //MONTHLY FIRST-MONDAY 12:45; FRIDAY 16:30
    //MONTHLY WORK-DAY 12:45;REST-DAY 16:30
    //MONTHLY WORK-DAY,REST-DAY 12:45; 16:30
    //MONTHLY WORK-DAY,SUNDAY 12,16:0/5
    //MONTHLY FIRST-WEEK WORK-DAY 12:45,30

    //FIRST-WEEK-WORK-DAY = E1W, LAST-WEEK-WORK-DAY = LEW
    //FIRST-WEEK-REST-DAY = E1R, LAST-WEEK-REST-DAY = LER
    //EnWEEK: E1MON = FIRST-MONDAY,E2w,E3w,E4w,E5,E6,LE

    //YEARLY JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC

    //FIRST-WEEK,SECOND-WEEK,THIRD-WEEK,FOURTH-WEEK,FIFTH-WEEK,LAST-WEEK
    //MONDAY,TUESDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,SUNDAY
    //FIRST-MONDAY,SECOND-MONDAY,THIRD-MONDAY,FOURTH-MONDAY,FIFTH-MONDAY,SIXTH-MONDAY,LAST-MONDAY
    //WORK-DAY,REST-DAY,LAST-DAY,LAST-WORK-DAY,LAST-REST-DAY

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

        exp
    }

    val period: String = {
        ChronExp.PERIOD.findFirstMatchIn(trimmed) match {
            case Some(m) => m.group(1)
            case None =>
                if ("""^\d""".r.test(trimmed) && trimmed.contains(" ")) {
                    trimmed.takeBefore(" ")
                }
                else {
                    throw new IllegalArgumentException("Empty or incorrect period type: " + expression + ", it must be MINUTELY/HOURLY/DAILY/WEEKLY/MONTHLY/YEARLY/ANNUAL or specific year.")
                }
        }
    }

    private val group: Array[String] = trimmed.takeAfter(ChronExp.PERIOD).split(";")
//    val matchable = new mutable.ArrayBuffer[Any]()
//    val statements = exp.split(";")
//    for (i <- statements.indices) {
//        matchable += statements(i)
//    }
//    matchable

    def toCron: List[CronExp] = {
        period match {
            case PeriodExp.MINUTELY => List[CronExp](CronExp())
            case PeriodExp.HOURLY =>
                group.map(s => {
                    CronExp(s"0 ${s.replaceAll("\\s", "")} * * * ? *")
                }).toList
            case PeriodExp.DAILY =>
                if (!group.head.contains(":")) {
                    throw new IllegalArgumentException("Illegal time format, must be HH:mm, like 09:10")
                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val time = group(i)
                    val hour = {
                        if (time.contains(":")) {
                            time.takeBefore(":")
                        }
                        else {
                            list(i-1).hour
                        }
                    }
                    val minute = {
                        if (time.contains(":")) {
                            time.takeAfter(":")
                        }
                        else {
                            time
                        }
                    }
                    list += CronExp(s"0 $minute $hour * * ? *")
                }
                list.toList
            case PeriodExp.WEEKLY =>
                if (!group.head.contains(" ")) {
                    throw new IllegalArgumentException("Illegal date time format, must be DAY HH:mm, like MON 09:10")
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
                    val hour = {
                        if (time.contains(":")) {
                            time.takeBefore(":")
                        }
                        else {
                            list(i-1).hour
                        }
                    }
                    val minute = {
                        if (time.contains(":")) {
                            time.takeAfter(":")
                        }
                        else {
                            time
                        }
                    }
                    list += CronExp(s"0 $minute $hour ? * $week *")
                }
                list.toList
            case PeriodExp.MONTHLY =>
                if (!group.head.contains(" ")) {
                    throw new IllegalArgumentException("Illegal date time format, must be DAY/WEEK HH:mm, like WORK-DAY 09:10")
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
                    val hour = {
                        if (time.contains(":")) {
                            time.takeBefore(":")
                        }
                        else {
                            list(i-1).hour
                        }
                    }
                    val minute = {
                        if (time.contains(":")) {
                            time.takeAfter(":")
                        }
                        else {
                            time
                        }
                    }

                    if ("""(MON|TUE|WED|THU|FRI|SAT|SUN|E)""".r.test(day)) {
                        list += CronExp(s"0 $minute $hour ? * $week *")
                    }
                    else {
                        list += CronExp(s"0 $minute $hour $day * ? *")
                    }
                }
                list.toList
            case _ =>
                if (!group.head.contains(" ") || group.head.split(" ").length != 3) {
                    throw new IllegalArgumentException("Illegal date time format, must be MONTH DAY/WEEK HH:mm, like JUN 01 09:10")
                }
                val list = new ListBuffer[CronExp]()
                for (i <- group.indices) {
                    val fields = group(i).split(" ")
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
                    val hour = {
                        if (time.contains(":")) {
                            time.takeBefore(":")
                        }
                        else {
                            list(i-1).hour
                        }
                    }
                    val minute = {
                        if (time.contains(":")) {
                            time.takeAfter(":")
                        }
                        else {
                            time
                        }
                    }

                    if ("""(MON|TUE|WED|THU|FRI|SAT|SUN|E)""".r.test(day)) {
                        list += CronExp(s"0 $minute $hour ? $month $day $period")
                    }
                    else {
                        list += CronExp(s"0 $minute $hour $day $month ? $period")
                    }
                }
                list.toList
        }
    }
}
