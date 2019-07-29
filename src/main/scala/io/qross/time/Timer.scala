package io.qross.time

import java.util.Calendar

object Timer {
    
    //sleep to next minute
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
    }
    
    //sleep to next second
    def rest(): Long = {
        val calendar = Calendar.getInstance
        Thread.sleep(1000 - calendar.get(Calendar.MILLISECOND))
        System.currentTimeMillis() / 1000
    }
}
