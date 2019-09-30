package io.qross.time

object TimeSpan {
    implicit class TimeSpan$Long(val milli: Long) {
        def toSeconds: Double = TimeSpan(milli).seconds
        def toMinutes: Double = TimeSpan(milli).minutes
        def toHours: Double = TimeSpan(milli).hours
        def toDays: Double = TimeSpan(milli).days
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
}