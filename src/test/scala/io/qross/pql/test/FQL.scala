package io.qross.pql.test

import java.net.{URLDecoder, URLEncoder}

import io.qross.core.DataHub
import io.qross.net.Json
import io.qross.pql.PQL
import io.qross.script.Shell.{destroy, end, kill, ps$ef}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.fs.ResourceFile

object FQL {
    def main(args: Array[String]): Unit = {


        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

        //PQL.openFile("/sql/test.sql").recognizeParameters().iterator().forEachRemaining(println)

//        Marker.openFile("/templates/markdown.md")
//            .transform()
//            .getContent
//            .print

        //new Marker(ResourceFile.open("/templates/markdown.md").content).transform().getContent.print

        PQL.openFile("/sql/args.sql").run()

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
