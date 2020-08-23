package io.qross.time

import io.qross.time

object TimeSpan {
    implicit class TimeSpan$Long(val milli: Long) {
        def toSeconds: Double = TimeSpan(milli).seconds
        def toMinutes: Double = TimeSpan(milli).minutes
        def toHours: Double = TimeSpan(milli).hours
        def toDays: Double = TimeSpan(milli).days

        def toTimeSpan(units: String = "d,h,m,s,ms"): String = TimeSpan(milli).span(units)
    }

    implicit class Timer$Double(val value: Double) {
        def millis: Long = value.round
        def seconds: Long = (value * 1000).round
        def minutes: Long = (value * 1000 * 60).round
        def hours: Long = (value * 1000 * 60 * 60).round
        def days: Long = (value * 1000 * 60 * 60 * 24).round
    }
}

case class TimeSpan(milli: Long) {
    def seconds: Double = milli.toDouble / 1000D
    def minutes: Double = milli.toDouble / 1000D / 60D
    def hours: Double = milli.toDouble / 1000D / 60D / 60D
    def days: Double = milli.toDouble / 1000D / 60D / 60D / 24D

    def span(units: String = "d,h,m,s,ms"): String = {
        val unit = units.split(",")

        if (milli > 3600000 * 24) {
            val day = milli / (3600000 * 24)
            val hour = milli % (3600000 * 24)
            if (hour > 0) {
                day + unit(0) + (hour / 3600000) + unit(1)
            }
            else {
                day + unit(0)
            }
        }
        else if (milli > 3600000) {
            val hour = milli / 3600000
            val minute = milli % 3600000
            if (minute > 0) {
                hour + unit(1) + (minute / 60000) + unit(2)
            }
            else {
                hour + unit(1)
            }
        }
        else if (milli > 60000) {
            val minute = milli / 60000
            val second = milli % 60000
            if (second > 0) {
                minute + unit(2) + (second / 1000) + unit(3)
            }
            else {
                minute + unit(2)
            }
        }
        else if (milli > 1000) {
            val second = milli / 1000
            if (milli % 1000 > 0) {
                second + unit(3) + (milli % 1000) + unit(4)
            }
            else {
                second + unit(3)
            }
        }
        else {
            milli + unit(4)
        }
    }
}