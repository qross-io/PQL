package io.qross.setting

import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean

object Environment {

    val cpuThreads: Int = Runtime.getRuntime.availableProcessors()

    def cpuUsage: Double = 0L

    def jvmUsedMemory: Long = {
        val runtime = Runtime.getRuntime
        runtime.totalMemory() - runtime.freeMemory()
    }

    def jvmMaxMemory: Long = {
        Runtime.getRuntime.maxMemory()
    }

    def jvmMemoryUsage: Float = {
        val runtime = Runtime.getRuntime
        val freeMemory: Long = runtime.freeMemory()
        val totalMemory: Long = runtime.totalMemory()
        val maxMemory: Long = runtime.maxMemory()
        ( Math.round((totalMemory.toDouble - freeMemory.toDouble) / maxMemory.toDouble * 10000D) / 10000D ).toFloat
    }

    def systemUsedMemory: Long = {
        val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
        bean.getTotalPhysicalMemorySize - bean.getFreePhysicalMemorySize
    }

    def systemTotalMemory: Long = {
        ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean].getTotalPhysicalMemorySize
    }

    def systemMemoryUsage: Float = {
        val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
        val freeMemory: Long = bean.getFreePhysicalMemorySize
        val totalMemory: Long = bean.getTotalPhysicalMemorySize
        println(freeMemory)
        println(totalMemory)

        (Math.round((totalMemory - freeMemory).toDouble / totalMemory.toDouble * 10000D) / 10000D).toFloat
    }
}
