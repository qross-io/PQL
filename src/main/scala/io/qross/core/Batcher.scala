package io.qross.core

import io.qross.jdbc.DataSource
import io.qross.time.Timer
import io.qross.ext._

class Batcher(source: DataSource, sentence: String) extends Thread {

    override def run(): Unit = {

        val ds = new DataSource(source.connectionName, source.databaseName)
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
                ds.tableUpdate(sentence, table)
            }

            Timer.sleep(0.1F)
        }

        ds.close()

        Output.writeMessage("Batcher Thread Exit!")
    }
}
