package cn.qross.core

import java.util.concurrent.ConcurrentLinkedQueue

import cn.qross.jdbc.DataSource
import cn.qross.thread.Cube
import cn.qross.time.Timer
import cn.qross.ext._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

//生产者-加工者-消费者模式中的加工者，可以有多个

object Processer {
    val CUBEs = new ArrayBuffer[Cube]()
    //存储生产者消费者处理模式中的中间结果（渠道）
    val DATA = new ArrayBuffer[ConcurrentLinkedQueue[DataTable]]()

    def closed: Boolean = {
        var ed = true
        breakable {
            for (cube <- CUBEs) {
                if (!cube.closed) {
                    ed = false
                    break
                }
            }
        }

        ed
    }

    def clear(): Unit = {
        CUBEs.clear()
        DATA.clear()
    }
}

class Processer(source: DataSource, sentence: String, index: Int, tanks: Int = 3) extends Thread {

    Processer.CUBEs += new Cube()
    if (Processer.DATA.size == index) {
        Processer.DATA += new ConcurrentLinkedQueue[DataTable]()
    }

    Processer.CUBEs(index).mark()

    override def run(): Unit = {

        val ds = new DataSource(source.config, source.databaseName)

        while (
            if (index == 0)
                !Pager.CUBE.closed || !Pager.DATA.isEmpty || !Blocker.CUBE.closed || !Blocker.DATA.isEmpty
            else
                !Processer.CUBEs(index-1).closed || !Processer.DATA(index - 1).isEmpty
        ) {
            val table: DataTable = if (index == 0) {
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
                Processer.DATA(index - 1).poll()
            }

            if (table != null) {
                Processer.DATA(index).add(ds.tableSelect(sentence, table))
            }

            do {
                Timer.sleep(100)
            }
            while (Processer.DATA(index).size() >= tanks)
        }

        ds.close()

        Processer.CUBEs(index).wipe()

        Output.writeMessage("Processer Thread Exit!")
    }
}
