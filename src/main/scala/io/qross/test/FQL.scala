package io.qross.test

import io.qross.app.Marker
import io.qross.core.DataHub
import io.qross.fql.SELECT
import io.qross.script.Shell._
import io.qross.script.Shell
import io.qross.ext.TypeExt._
import io.qross.pql.{PQL, Patterns}
import io.qross.pql.Solver._
import io.qross.pql.Solver.USER_VARIABLE
import io.qross.setting.{Environment, Global}
import io.qross.time.{ChronExp, CronExp, DateTime}
import io.qross.fs.Path._
import io.qross.net.Json

import scala.sys.process._
import scala.util.matching.Regex

object FQL {
    def main(args: Array[String]): Unit = {
        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

//        Marker.openFile("c:/io.Qross/Folder/pql/overview.md")
//            .replaceInMarkdown("](/", "[(/doc/")
//            .replaceInMarkdown(".md)", ")")
//            .transform()
//            .removeHeader()
//            .colorCodes(false)
//            .getContent
//            .print

        DateTime.now.getTickValue.print

        System.exit(0)

        val row = new Json("""{"action_id":5089,"job_id":2,"task_id":3584,"command_id":26,"task_time":"20201012112122","record_time":"2020-10-12 11:21:22","start_mode":"manual_start","command_type":"shell","command_text":"java -jar hello.jar ${ $task_time FORMAT \"yyyy-MM-dd\" }","overtime":0,"retry_limit":0,"job_type":"scheduled","title":"Test Job 2","owner":"吴佳妮<wujini@outlook.com>"}""").parseRow("/")
        val text = "java -jar hello.jar ${ $task_time FORMAT \"yyyy-MM-dd\" }"
        text.$restore(new PQL("", DataHub.DEFAULT).set(row), "").print

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