package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.jdbc.DataSource
import io.qross.thread.Cube
import io.qross.time.{DateTime, Timer}
import io.qross.ext._

object Blocker {
    val CUBE = new Cube()
    val QUEUE = new ConcurrentLinkedQueue[String]()
    val DATA = new ConcurrentLinkedQueue[DataTable]()

    var working = 0
    def plus(): Unit = synchronized {
        working += 1
    }
    def minus(): Unit = synchronized {
        working -= 1
    }
}

class Blocker(source: DataSource, tanks: Int, dh: DataHub) extends Thread {

    //活跃线程 +1, 线程创建时加 1 - 在线程内部判断时使用
    Blocker.CUBE.mark()

    override def run(): Unit = {

        Timer.sleepRandom(50)

        val ds = new DataSource(source.config, source.databaseName)

        while (!Blocker.QUEUE.isEmpty) {

            val SQL = Blocker.QUEUE.poll()
            if (SQL != null) {

                while (Blocker.working >= tanks || Blocker.DATA.size() >= tanks) {
                    Timer.sleepRandom(100)
                }

                Blocker.plus()
                while (Blocker.working > tanks) {
                    Timer.sleepRandom(100)
                }
                val table = ds.executeDataTable(SQL)
                Blocker.DATA.add(table)
                Blocker.minus()

                //println("DATA SIZE: " + Blocker.DATA.size())

                dh.COUNT_OF_LAST_GET = table.size
                dh.TOTAL_COUNT_OF_RECENT_GET += dh.COUNT_OF_LAST_GET

                if (dh.debugging) {
                    println(s"""Get Data ${dh.COUNT_OF_LAST_GET}, Total Get ${dh.TOTAL_COUNT_OF_RECENT_GET}.""")
                }
            }
        }

        ds.close()

        Blocker.CUBE.wipe()
    }
}
