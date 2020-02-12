package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.jdbc.DataSource
import io.qross.thread.Cube
import io.qross.time.Timer
import io.qross.ext._

object Blocker {
    val CUBE = new Cube()
    val QUEUE = new ConcurrentLinkedQueue[String]()
    val DATA = new ConcurrentLinkedQueue[DataTable]()
}

class Blocker(source: DataSource, tanks: Int = 3) extends Thread {

    //活跃线程+1, 线程创建时加1 - 在线程内部判断时使用
    Blocker.CUBE.mark()

    override def run(): Unit = {


        val ds = new DataSource(source.connectionName, source.databaseName)

        while (!Blocker.QUEUE.isEmpty) {
            val SQL = Blocker.QUEUE.poll()
            if (SQL != null) {
                Blocker.DATA.add(ds.executeDataTable(SQL))
            }

            while (Blocker.DATA.size() >= tanks) {
                Timer.sleep(100)
            }
        }

        ds.close()

        Blocker.CUBE.wipe()

        Output.writeMessage("Blocker Thread Exit!")
    }
}
