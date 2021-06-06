package io.qross.pql.test

import io.qross.ext.TypeExt._
import io.qross.ext.Console
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.pql.PQL
import io.qross.time.{ChronExp, DateTime}

object Enter {
    def main(args: Array[String]): Unit = {


        PQL.openFile("/test.sql").place("""{
                                          |"pagesize":"1000",
                                          |"source_database_name":"PostgresSQL",
                                          |"source_connection_name":"postgresql.test",
                                          |"start_point":"SELECT MIN(id)-1 FROM test.data_test_30000",
                                          |"end_point":"SELECT MAX(id) FROM test.data_test_30000",
                                          |"block_sentence":"SELECT * FROM test.data_test_30000 WHERE id>@{id} AND id<=@{id};",
                                          |"destination_database_name":"MySQL",
                                          |"destination_connection_name":"mysql.test",
                                          |"prep_sentence":"delete from test.data_test1_30000;",
                                          |"batch_sentence":"insert into test.data_test1_30000(ID,NAME,AGE,ADDRESS,SALARY,JOIN_DATE) values(?,?,?,?,?,?);"
                                          |}""".stripMargin).run()

        System.exit(0)

        PQL.openFile("c:/io.Qross/Keeper/src/main/resources/pql/metadata.sql")
            .place("prefix=qross")
            .run()

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
