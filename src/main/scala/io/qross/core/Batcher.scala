package io.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import io.qross.jdbc.DataSource
import io.qross.thread.Cube
import io.qross.time.Timer

class Batcher(source: DataSource, sentence: String, dh: DataHub) extends Thread {

    override def run(): Unit = {

        val ds = new DataSource(source.config, source.databaseName)
        val index = Processer.DATA.size - 1
        while (
            if (index == -1)
                !Pager.CUBE.closed || !Pager.DATA.isEmpty || !Blocker.CUBE.closed || !Blocker.DATA.isEmpty
            else
                !Processer.closed
        ) {
            val table: DataTable =
                if (index == -1) {
                    if (!Pager.DATA.isEmpty) {
                        Pager.DATA.poll()
                    }
                    else if (!Blocker.DATA.isEmpty) {
                        Blocker.DATA.poll()
                    }
                    else {
                        null
                    }
                }
                else {
                    Processer.DATA(index).poll()
                }

            if (table != null) {
                dh.AFFECTED_ROWS_OF_LAST_PUT = ds.tableUpdate(sentence, table)
                table.clear()
                if (dh.AFFECTED_ROWS_OF_LAST_PUT > 0) {
                    dh.TOTAL_AFFECTED_ROWS_OF_RECENT_PUT += dh.AFFECTED_ROWS_OF_LAST_PUT
                }

                if (dh.debugging) {
                    println(s"""Transfer Data ${dh.AFFECTED_ROWS_OF_LAST_PUT}, Total Transfer ${dh.TOTAL_AFFECTED_ROWS_OF_RECENT_PUT}.""")
                }
            }

            Timer.sleepRandom(50)
        }

        ds.close()
    }
}
