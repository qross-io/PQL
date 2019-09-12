package io.qross.ext

object NumberExt {
    implicit class FloatExt(float: Float) {
        def floor(precision: Int = 0): Double = {
            Math.floor(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(float * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def percent: String = {
            s"${float * 100}%"
        }

        def pow(p: Double = 2): Double = Math.pow(float, p)
    }

    implicit class DoubleExt(double: Double) {
        def floor(precision: Int = 0): Double = {
            Math.floor(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def round(precision: Int = 0): Double = {
            Math.round(double * Math.pow(10, precision)) / Math.pow(10, precision)
        }

        def percent: String = {
            s"${double * 100}%"
        }

        def pow(p: Double = 2): Double = Math.pow(double, p)
    }
}
