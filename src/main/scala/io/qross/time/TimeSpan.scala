package io.qross.time

object TimeSpan {
    implicit class TimeSpan$Long(val milli: Long) {
        def seconds: Double = TimeSpan(milli).seconds
        def minutes: Double = TimeSpan(milli).minutes
        def hours: Double = TimeSpan(milli).hours
        def days: Double = TimeSpan(milli).days
    }
}

case class TimeSpan(milli: Long) {
    def seconds: Double = milli.toDouble / 1000D
    def minutes: Double = milli.toDouble / 1000D / 60D
    def hours: Double = milli.toDouble / 1000D / 60D / 60D
    def days: Double = milli.toDouble / 1000D / 60D / 24D
}