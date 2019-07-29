package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.jdbc.DataSource
import io.qross.time.Timer

object Bulker {
    val QUEUE = new ConcurrentLinkedQueue[String]()
}

class Bulker(source: DataSource) extends Thread {

    override def run(): Unit = {
        val ds = new DataSource(source.connectionName, source.databaseName)
        while (!Bulker.QUEUE.isEmpty) {
            val SQL = Bulker.QUEUE.poll()
            if (SQL != null) {
                ds.executeNonQuery(SQL)
                println(SQL)
            }
            println("运行状态++++++++++++++++++++++++++")
            println(Bulker.QUEUE.size())
            Timer.sleep(0.1F)
        }
        ds.close()
    }
}
