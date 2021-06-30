package io.qross.time

import java.util.Calendar

import scala.util.Random

object Timer {

    //sleep to next minute
    /*
    def sleep(seconds: Float = 0F): Unit = {
        try {
            if (seconds > 0) {
                Thread.sleep((seconds * 1000).round)
            }
            else {
                val calendar = Calendar.getInstance
                Thread.sleep((60000 - calendar.get(Calendar.SECOND) * 1000 - calendar.get(Calendar.MILLISECOND) - seconds * 1000).round)
            }
        }
        catch {
            case e: InterruptedException => e.printStackTrace()
        }
    } */

    def sleep(millis: Long): Unit = {
        try {
            Thread.sleep(if (millis > 5) millis else Random.nextInt(50))
        }
        catch {
            case e: Exception => e.printStackTrace()
                //InterruptedException
        }
    }
    
    //sleep to next second and return epoch second
    def rest(): Long = {
        sleepToNextSecond()
        System.currentTimeMillis() / 1000
    }

    def sleepRandom(ms: Int): Unit = {
        sleep(Random.nextInt(ms))
    }

    def sleepToNextSecond(): Unit = {
        sleep(1000 - Calendar.getInstance.get(Calendar.MILLISECOND))
    }

    def sleepToNextMinute(): Unit = {
        val calendar =Calendar.getInstance
        sleep(60000 - calendar.get(Calendar.SECOND) * 1000 - calendar.get(Calendar.MILLISECOND))
    }
}
