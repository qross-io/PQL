package io.qross.test

import io.qross.setting.Configurations
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.server
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import io.qross.core.DataHub
import io.qross.pql.PQL

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Rest {
    def main(args: Array[String]): Unit = {

        implicit val system: ActorSystem = ActorSystem("api-server")
        implicit val materializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher

        val path: server.Route =
            pathSingleSlash {
                get {
                    complete(new PQL("SELECT * FROM qross_jobs LIMIT 5", DataHub.QROSS).run().toString)
                }
            }

        val bindingFuture = Http().bindAndHandle(path,"0.0.0.0",8250)
        println("Server is running...")
        StdIn.readLine()
        bindingFuture.flatMap(_.unbind()).onComplete(_=>system.terminate())
        println("Server has shut down.")
    }
}
