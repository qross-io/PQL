package io.qross.time

import io.qross.ext.TypeExt._
import io.qross.time.ChronExp._

import scala.util.matching.Regex

object ChronExp {

    //val LUNAR: Regex = """(?i)\sLUNAR\s""".r
    val PERIOD: Regex = """(?i)^(MINUTELY|HOURLY|DAILY|WEEKLY|MONTHLY|ANNUAL|YEARLY)\s""".r

    val CLASSIC: String = "CLASSIC"
}

case class ChronExp(expression: String) {

    val classic: Boolean = !PERIOD.test(expression)

    private val group: List[CronExp] = {
        if (classic) {
            //经典cron表达式, 支持分号分开的多个
            expression.split(";").map(e => CronExp(e.trim())).toList
        }
        else {
            PeriodExp(expression.trim()).toCron
        }
    }

    def getNextTick(dateTime: DateTime): Option[DateTime] = {

        group
            .map(cron => cron.getNextTick(dateTime))
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

        None
    }
}
