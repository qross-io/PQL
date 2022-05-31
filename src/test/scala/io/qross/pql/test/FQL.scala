package io.qross.pql.test

import java.net.{URLDecoder, URLEncoder}

import io.qross.core.DataHub
import io.qross.ext.Output.writeException
import io.qross.net.Json
import io.qross.pql.PQL
import io.qross.script.Shell.{destroy, end, kill, ps$ef}
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._
import io.qross.fs.{Directory, ResourceFile}
import io.qross.jdbc.DataSource
import io.qross.setting.{Environment, Global}
import io.qross.time.{ChronExp, DateTime}

import scala.collection.mutable

object FQL {
    def main(args: Array[String]): Unit = {


        //new SELECT("select * from :hello where name='Tom' and age>18 order by age desc limit 10")

        //PQL.openFile("/sql/test.sql").recognizeParameters().iterator().forEachRemaining(println)

//        Marker.openFile("/templates/markdown.md")
//            .transform()
//            .getContent
//            .print

        //new Marker(ResourceFile.open("/templates/markdown.md").content).transform().getContent.print

        //PQL.openFile("/pql.sql").signIn(1, "admin", "1").place("module_name=数据质量&stars=1").placeDefault("rules=").run().print
        
//        val x = System.currentTimeMillis()
//        PQL.openFile("""c:/io.Qross/Home/metadata.sql""").place("connection_name", "cs_mysql").run()
//        println(System.currentTimeMillis() - x)

        //val select = DataSource.QROSS.querySingleValue("SELECT info FROM td WHERE id=100").asText("")
        PQL.openFile("/test.sql").run()
        //PQL.openFile("c:/io.Qross/Home/data/calendar.sql").run()

        //DataSource.testConnection("MySQL", "com.mysql.cj.jdbc.Driver", "jdbc:mysql://39.99.240.254:3306", "root", "root", "eeeeee").print

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
