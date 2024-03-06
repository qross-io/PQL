package cn.qross.pql

import cn.qross.core.{DataCell, DataTable, DataType}
import cn.qross.ext.TypeExt._
import cn.qross.net.Redis._
import cn.qross.pql.Patterns.ARROW
import cn.qross.pql.Solver._

import scala.collection.JavaConverters._

object REDIS {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("REDIS", sentence, new REDIS(sentence)))
    }
}

class REDIS(val sentence: String) {

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, redis => PQL.dh.command(redis))
    }

    //用于pass语句中
    def evaluate(PQL: PQL, table: DataTable): DataCell = {

        var body = sentence.$clean(PQL)
        val links = {
            if (body.contains(ARROW)) {
                body.takeAfter(ARROW)
            }
            else {
                ""
            }
        }

        if (links != "") {
            body = body.takeBefore(ARROW).trim()
        }

        val data = PQL.dh.pipelined(body.popStash(PQL), table)

        if (links != "") {
            new Sharp(links, {
                //如果是list, 则强制回转, 以便能再计算
                if (data.width == 1) {
                    data.firstColumn match {
                        case Some(list) => list.asJava.toDataCell(DataType.ARRAY)
                        case _ => data.toDataCell(DataType.TABLE)
                    }
                }
                else {
                    data.toDataCell(DataType.TABLE)
                }
            }).execute(PQL)
        }
        else {
            data.toDataCell(DataType.TABLE)
        }
    }

    def execute(PQL: PQL): Unit = {
        val data = this.evaluate(PQL)

        PQL.WORKING += data.value

        if (PQL.dh.debugging) {
            data.dataType match {
                case DataType.TABLE => data.asTable.show()
                case _ => println(data.asText)
            }
        }
    }
}
