package io.qross.test

import io.qross.app.Marker
import io.qross.fql.SELECT
import io.qross.script.Shell._
import io.qross.script.Shell
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns
import io.qross.pql.Solver.USER_VARIABLE
import io.qross.setting.Global
import io.qross.time.{ChronExp, CronExp, DateTime}
import io.qross.fs.Path._

import scala.sys.process._
import scala.util.matching.Regex

object FQL {
    def main(args: Array[String]): Unit = {
        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

        DateTime.now.plusDays(2).getDayOfWeek.print

//        Marker.openFile("c:/io.Qross/Folder/pql/overview.md")
//            .replaceInMarkdown("](/", "[(/doc/")
//            .replaceInMarkdown(".md)", ")")
//            .transform()
//            .removeHeader()
//            .colorCodes(false)
//            .getContent
//            .print



        if (args.nonEmpty) {
            args(0) match {
                case "ps" =>
                    ps$ef("qross-test").foreach(println)
                case "destroy" =>
                    println(destroy("test.sh"))
                case "end" =>
                    println(end("test.sh"))
                case _ =>
                    println(kill(args(0).toInt))
            }
        }
        else {
            //println("LACK OF ARGS.")
        }
    }
}