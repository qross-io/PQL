package io.qross.pql.test

import io.qross.ext.TypeExt._

object Enter {
    def main(args: Array[String]): Unit = {
        io.qross.pql.PQL.runFile("/test.sql")

        """<div class="feature">
          |<div class="feature"><a -href="/job/detail?jobId=&(jobId)"># home #</a></div>
          |<div class="feature"><a -href="/job/tasks?jobId=&(jobId)"># tasks #</a> &nbsp; <span class="gray">(<%=$job.tasks%>)</div>
          |</div>""".stripMargin.stackPairOf("<div", "</div>", 0).print

        System.exit(0)

        """
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
          |""".stripMargin.stackPairOf("<div", "</div>", 0).print

//        Class.forName("io.qross.pql.GlobalFunction")
//            .getDeclaredMethods
//            .foreach(println)
            //.getDeclaredMethod("putX", classOf[java.lang.String], classOf[java.lang.String], classOf[java.lang.String], classOf[java.lang.Integer])
            //.invoke(null, "TEST", "", "", Integer.valueOf(0))
    }
}
