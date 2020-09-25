package io.qross.time

import io.qross.ext.TypeExt._
import io.qross.time.ChronExp._

import scala.collection.mutable
import scala.util.matching.Regex

object ChronExp {

    //val LUNAR: Regex = """(?i)\sLUNAR\s""".r
    val PERIOD: Regex = """(?i)^(MINUTELY|HOURLY|DAILY|WEEKLY|MONTHLY|ANNUAL|YEARLY|[12]\d{3})\s""".r
    val CLASSIC: String = "CLASSIC"

    def getTicks(chronExp: String, begin: String, end: String): List[String] = {
        var tick = new DateTime(begin)
        val terminal = new DateTime(end)
        val ticks = new mutable.ListBuffer[String]()
        val chron = new ChronExp(chronExp)
        while(tick.beforeOrEquals(terminal)) {
            chron.getNextTick(tick) match {
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
}

case class ChronExp(expression: String) {

    private val exps = {
        if (PERIOD.test(expression)) {
            PeriodExp(expression.trim()).toCron
        }
        else {
            //经典cron表达式, 支持分号分开的多个
            expression.split("[;；]").map(e => CronExp(e.trim())).toList
        }
    }

    def getNextTick(dateTime: String): Option[DateTime] = {
        getNextTick(new DateTime(dateTime))
    }

    def getNextTick(dateTime: DateTime): Option[DateTime] = {
        exps.map(cron => cron.getNextTick(dateTime))
        .reduce((r1, r2) => {
            (r1, r2) match {
                case (Some(t1), Some(t2)) => {
                    if (t1.beforeOrEquals(t2)) {
                        Some(t1)
                    }
                    else {
                        Some(t2)
                    }
                }
                case (Some(t1), None) => Some(t1)
                case (None, Some(t2)) => Some(t2)
                case (None, None) => None
            }
        })
    }

    def matches(dateTime: DateTime): Boolean = {
        exps.exists(cron => cron.matches(dateTime))
    }

    def matches(dateTime: String): Boolean = {
        matches(new DateTime(dateTime))
    }

    def getNextTickOrNone(dateTime: String): String = {
        getNextTickOrNone(new DateTime(dateTime))
    }

    def getNextTickOrNone(dateTime: DateTime): String = {
        getNextTick(dateTime) match {
            case Some(tick) => tick.getTickValue
            case None => "N/A"
        }
    }
}
