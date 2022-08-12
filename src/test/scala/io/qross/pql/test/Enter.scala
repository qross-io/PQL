package io.qross.pql.test

import java.time.ZoneId

import io.qross.ext.TypeExt._
import io.qross.ext.Console
import io.qross.fs.{FileReader, FileWriter, SourceFile, TextFile}
import io.qross.jdbc.DataSource
import io.qross.net.{Email, Json}
import io.qross.pql.{PQL, Syntax}
import io.qross.time.{ChronExp, DateTime}
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.io.Source

object Enter {
    def main(args: Array[String]): Unit = {

//        val data = """1970,1,1,廿四,,元旦,4,-1
//          |1970,2,12,初七,,,4,-1
//          |1970,2,13,初八,,,5,-1
//          |1970,2,14,初九,,情人节,6,-1
//          |1970,2,18,十三,,,3,-1
//          |1970,2,19,十四,雨水,,4,-1
//          |1970,2,20,十五,,元宵节,5,-1
//          |1970,2,21,十六,,,6,-1
//          |1970,3,5,廿八,,,4,-1
//          |1970,3,6,廿九,惊蛰,,5,-1""".stripMargin
//
//        """(?m)^(\d{4},\d{1,2},\d{1,2}),([^,]+),([^,]*),([^,]*)(,\d,-?\d)$""".r.findAllMatchIn(data)
//                .map(a => "(" + a.group(1) + ",'" + a.group(2) + "','" + a.group(3) + "','" + a.group(4) + "'" + a.group(5) + ")")
//                .foreach(println)


        ZoneId.getAvailableZoneIds.asScala.filter(x => x.contains("Etc/GMT")).foreach(println)

        System.exit(0)

        Json("""{
        "direction": "in","question": "Ignore  me.","iffy_regex": "\('([a-z]+)', '([a-z]+)'\)","origin_text":  "[('the', 'DT'), ('little', 'JJ'), ('yellow', 'JJ'), ('dog', 'NN'), ('died', 'VBD')]","email": "wu@qross.io"
        }""").parseRow("/").print


        System.exit(0)

        val reader = new FileReader("c:/io.Qross/Home/data/calendar.csv", format = TextFile.CSV)
        val writer = new FileWriter("c:/io.Qross/Home/data/calendar.sql", deleteIfExists = true)
        var count = 0
        val sb = new mutable.StringBuilder()

        while(reader.hasNextLine) {
            val fields = reader.readLine().split(",", -1)

            if (count % 1000 == 0) {
                sb.append("INSERT INTO qross_calendar (solar_year, solar_month, solar_day, lunar_day, solar_term, festival, week_number, workday) VALUES ")
            }
            else {
                sb.append(",")
            }

            sb.append("(" + fields(0).trim() + "," + fields(1) + "," + fields(2) + ",'" + fields(3) + "','" + fields(4) + "','" + fields(5) + "'," + fields(6) + "," + fields(7) + ")")

            if (count % 1000 == 999) {
                sb.append(";")
                writer.writeLine(sb.toString())
                sb.clear()
            }

            count += 1
        }

        if (sb.nonEmpty) {
            sb.append(";")
            writer.writeLine(sb.toString())
            sb.clear()
        }

        writer.close()
        reader.close()

        System.exit(0)

        //PQL.openFile("/sql/args.sql").place("""source_database_name=MySQL&source_connection_name=mysql.test&source_where=&source_columns="id","name","age","address","salary"&destination_connection_name=test&source_splitPk=&source_table=data_test1_30000&source_ddl=&target_database_name=PostgreSQL&target_connection_name=postgresql.test&target_preSql=&target_columns="id","name","age","address","salary"&target_postSql=&target_table=test.data_test_30000""").run()

        val sh = PQL.openEmbedded("""python /usr/local/datax/bin/datax.py <%=FILE VOYAGE '''@QROSS_HOME/pql/datax.json''' WITH @ARGUMENTS TO '''@QROSS_HOME/temp/test.json''' %>""").place("""source_database_name=MySQL&source_connection_name=mysql.test&source_where=&source_columns="id","name","age","address","salary"&destination_connection_name=test&source_splitPk=&source_table=data_test1_30000&source_ddl=&target_database_name=PostgreSQL&target_connection_name=postgresql.test&target_preSql=&target_columns="id","name","age","address","salary"&target_postSql=&target_table=test.data_test_30000""").run().asInstanceOf[String]
        println(sh)

        System.exit(0)

//        PQL.openFile("/api/test.sql").place("module_name=数据集成&project_name=数据计算").placeDefault("module_name=&parent=0").run().print

        PQL.recognizeParametersInEmbedded("python datax.py --json <%=FILE VOYAGE '''@QROSS_HOME/pql/datax.json''' WITH '#{params}' TO '''@QROSS_HOME/temp/$task_id/$(action_id).json''' %>")
                .forEach(println)

        System.exit(0)

        val content = """
          |<# page template="/template/form.html" />
          |@{
          |    "language": "system",
          |    "previous": "当前命令模板",
          |    "button": "当前命令模板",
          |    "button_class": "prime-button",
          |    "crumb": "设置命令模板参数",
          |    "jump": "/system/command-template-detail?id=#{id}",
          |    "back": "/system/command-template-detail?id=<%=$id%>"
          |}
          |<div class="features">
          |    <div class="feature"><a -href="/job/detail?jobId=&(jobId)"># home #</a></div>
          |    <div class="feature"><a -href="/job/tasks?jobId=&(jobId)"># tasks #</a> &nbsp; <span class="gray">(<%=$job.tasks%>)</div>
          |    <% IF $rule.RUN_JOB_AT_ONCE == 'yes' THEN %>
          |    <div class="feature run-at-once"><a -href="/job/manual?jobId=&(jobId)"># run-at-once #</a></div>
          |    <% END IF %>
          |    <div class="feature-focus"># dag #</div>
          |    <div class="feature"><a -href="/job/depends?jobId=&(jobId)"># depends #</a> &nbsp; <span class="gray">(<%=$job.depends%>)</div>
          |    <div class="feature"><a -href="/job/events?jobId=&(jobId)"># events #</a> &nbsp; <span class="gray">(<%=$job.events%>)</div>
          |    <div class="feature"><a -href="/job/owner?jobId=&(jobId)"># owners #</a> &nbsp; <span class="gray">(<%=$job.owners%>)</div>
          |</div>
          |""".stripMargin

        Test.hello(content)

            //.stackPairOf("<div", "</div>", 0).print

//        Class.forName("io.qross.pql.GlobalFunction")
//            .getDeclaredMethods
//            .foreach(println)
            //.getDeclaredMethod("putX", classOf[java.lang.String], classOf[java.lang.String], classOf[java.lang.String], classOf[java.lang.Integer])
            //.invoke(null, "TEST", "", "", Integer.valueOf(0))
    }
}
