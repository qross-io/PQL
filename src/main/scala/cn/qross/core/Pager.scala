package cn.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import cn.qross.jdbc.DataSource
import cn.qross.thread.Cube
import cn.qross.time.Timer
import cn.qross.ext.Output

object Pager {
    val CUBE = new Cube()
    val DATA: ConcurrentLinkedQueue[DataTable] = new ConcurrentLinkedQueue[DataTable]()
}

class Pager(source: DataSource,
            selectSQL: String,
            param: String = "@{offset}",
            pageSize: Int = 10000,
            tanks: Int,
            dh: DataHub) extends Thread {

    //活跃线程+1, 线程创建时加1 - 在线程内部判断时使用
    Pager.CUBE.mark()

    override def run(): Unit = {

        val ds = new DataSource(source.config, source.databaseName)
        var break = false
        do {
            while (Pager.DATA.size() >= tanks) {
                Timer.sleep(200)
            }

            val table = ds.executeDataTable(selectSQL.replace(param, String.valueOf(Pager.CUBE.increase() * pageSize)))

            //无数据也得执行，以保证会正确创建表
            Pager.DATA.add(table)

            dh.COUNT_OF_LAST_GET = table.size
            dh.TOTAL_COUNT_OF_RECENT_GET += dh.COUNT_OF_LAST_GET

            if (table.isEmpty) {
                break = true
            }

        } while (!break)

        ds.close()

        //活跃线程, 在线程关闭时减1
        Pager.CUBE.wipe()
        Output.writeMessage("Pager Thread Exit!")
    }
}
