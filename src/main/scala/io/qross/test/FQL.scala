package io.qross.test

import io.qross.app.Marker
import io.qross.fql.SELECT
import io.qross.script.Shell._
import io.qross.script.Shell
import io.qross.ext.TypeExt._
import io.qross.pql.Patterns
import io.qross.pql.Solver.USER_VARIABLE
import io.qross.setting.Global
import io.qross.time.{ChronExp, DateTime}

import scala.sys.process._
import scala.util.matching.Regex

object FQL {
    def main(args: Array[String]): Unit = {
        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")


        val sentence = """SELECT id, record_time FROM qross_tasks WHERE job_id=$job_id.name AND id<=$task_id(1) UNION SELECT task_id, record_time FROM qross_tasks_records WHERE job_id=$job_id[1] AND task_id<=$task_id"""
            List[Regex](
                """\$\(([a-zA-Z0-9_]+)\)""".r,
                """\$([a-zA-Z0-9_]+)\b(?![(.\[])""".r
            )
            .map(r => r.findAllMatchIn(sentence))
            .flatMap(s => s.toList.sortBy(m => m.group(0)).reverse)  //反转很重要, $user  $username 必须先替换长的
            .foreach(m => println(m.group(0) + "#"))

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