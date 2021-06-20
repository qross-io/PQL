package io.qross.pql.test

import io.qross.ext.TypeExt._
import io.qross.ext.Console
import io.qross.jdbc.DataSource
import io.qross.pql.PQL
import io.qross.time.{ChronExp, DateTime}

object Enter {
    def main(args: Array[String]): Unit = {

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
