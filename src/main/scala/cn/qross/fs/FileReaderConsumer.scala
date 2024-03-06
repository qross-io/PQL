package cn.qross.fs

import cn.qross.core.DataTable
import cn.qross.thread.Cube
import cn.qross.time.Timer

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
