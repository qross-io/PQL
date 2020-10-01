package io.qross.setting

import java.lang.management.ManagementFactory
import java.net.InetAddress

import com.sun.management.OperatingSystemMXBean
import io.qross.time.Timer
import io.qross.ext.TypeExt._

object Environment {

    val cpuThreads: Int = Runtime.getRuntime.availableProcessors()

    def cpuUsage: Double = {
        val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]

        var total = 0D
        for (i <- 0 to 99) {
            Timer.sleep(10)
            total += bean.getSystemCpuLoad
        }

        total / 100
    }

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

        (Math.round((totalMemory - freeMemory).toDouble / totalMemory.toDouble * 10000D) / 10000D).toFloat
    }

    def localHostAddress: String = InetAddress.getLocalHost.getHostAddress

    def runningDirectory: String = {
        val sameDir = BaseClass.MAIN.getProtectionDomain.getCodeSource.getLocation.getPath
        //spring boot - Master
        if (sameDir.contains(".jar!")) {
            sameDir.takeAfter("file:").takeBefore(".jar!").takeBeforeLast("/") + "/"
        }
        //一般jar包 - Keeper
        else if (sameDir.endsWith(".jar")) {
            sameDir.takeBeforeLast("/") + "/"
        }
        else {
            sameDir.takeBefore("/classes") + "/"
            //new File(sameDir).getParentFile.getAbsolutePath.replace("\\", "/") + "/" + fileName
        }
    }
}
