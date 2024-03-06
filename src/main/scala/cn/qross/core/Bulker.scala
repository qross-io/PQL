package cn.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import cn.qross.jdbc.DataSource
import cn.qross.time.Timer

object Bulker {
    val QUEUE = new ConcurrentLinkedQueue[String]()
}

class Bulker(source: DataSource) extends Thread {

    override def run(): Unit = {
        val ds = new DataSource(source.config, source.databaseName)
        while (!Bulker.QUEUE.isEmpty) {
            val SQL = Bulker.QUEUE.poll()
            if (SQL != null) {
                ds.executeNonQuery(SQL)
            }
            Timer.sleep(100)
        }
        ds.close()
    }
}
