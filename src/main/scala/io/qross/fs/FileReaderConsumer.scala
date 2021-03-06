package io.qross.fs

import io.qross.core.DataTable
import io.qross.thread.Cube
import io.qross.time.Timer

class FileReaderConsumer(cube: Cube, handler: DataTable => Unit) extends Thread {

    override def run(): Unit = {
        while (!cube.closed || !FileReader.DATA.isEmpty) {
            val table = FileReader.DATA.poll()
            if (table != null) {
                handler(table)
            }
            table.clear()

            Timer.sleep(100)
        }
    }
}
