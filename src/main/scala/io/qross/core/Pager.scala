package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.jdbc.DataSource
import io.qross.thread.Cube
import io.qross.time.Timer
import io.qross.ext.Output

object Pager {
    val CUBE = new Cube()
    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()
}

class Pager(source: DataSource,
            selectSQL: String,
            param: String = "#offset",
            pageSize: Int = 10000, tanks: Int = 3) extends Thread {

    //线程创建时加1 - 在线程内部判断时使用
    Pager.CUBE.mark()

    override def run(): Unit = {

        val ds = new DataSource(source.connectionName, source.databaseName)
        var break = false
        do {
            while (Pager.DATA.size() >= tanks) {
                Timer.sleep(0.5F)
            }

            val table = ds.executeDataTable(selectSQL.replace(param, String.valueOf(Pager.CUBE.increase() * pageSize)))

            //无数据也得执行，以保证会正确创建表
            Pager.DATA.add(table)

            if (table.isEmpty) {
                break = true
            }

        } while (!break)

        ds.close()

        //线程关闭时减1
        Pager.CUBE.wipe()
        Output.writeMessage("Pager Thread Exit!")
    }
}
