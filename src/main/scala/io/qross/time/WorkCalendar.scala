package io.qross.time

import io.qross.jdbc.{DataSource, JDBC}
import io.qross.ext.TypeExt._

import scala.collection.mutable

object WorkCalendar {

    //date -> isWorkday
    val DAYS = new mutable.HashMap[String, Boolean]()

    def loadCalendar(year: String): Unit = {
        if (JDBC.hasQrossSystem) {
            DataSource.QROSS.queryDataTable("SELECT CONCAT(solar_year, '-', IF(solar_month < 10, '0', ''), solar_month, '-', IF(solar_day < 10, '0', ''), solar_day) AS solar_date, week_number, workday FROM qross_calendar WHERE solar_year=" + year)
                .foreach(row => {
                    DAYS += row.getString("solar_date") -> (if (row.getInt("workday") == -1) row.getInt("week_number") < 6 else row.getInt("workday") == 1)
                }).clear()
        }
    }

    def isWorkday(date: String): Boolean = {
        if (!DAYS.contains(date)) {
            loadCalendar(date.takeBefore("-"))
        }

        if (DAYS.contains(date)) {
            DAYS(date)
        }
        else {
            new DateTime(date).getDayOfWeek < 6
        }
    }

    def isWorkday(datetime: DateTime): Boolean = {
        val date = datetime.getString("yyyy-MM-dd")
        if (!DAYS.contains(date)) {
            loadCalendar(date.takeBefore("-"))
        }

        if (DAYS.contains(date)) {
            DAYS(date)
        }
        else {
            datetime.getDayOfWeek < 6
        }
    }
}

//val lunarMonth: String, val lunarDay: String, val solarTerm: String, val festival: String,
//class CalendarCell(val week: Int, val workday: Int) {
//    def isWorkday: Boolean = {
//        if (workday == -1) {
//            week < 6
//        }
//        else {
//            workday == 1
//        }
//    }
//
//    def isHoliday: Boolean = {
//        if (workday == -1) {
//            week < 6
//        }
//        else {
//            workday == 1
//        }
//    }
//}
