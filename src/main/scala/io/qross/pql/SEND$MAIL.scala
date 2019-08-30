package io.qross.pql

import io.qross.net.Email
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

import scala.collection.mutable

class SEND$MAIL(val info: String) {

    //SET CONTENT
    //FROM TEMPLATE
    //FROM DEFAULT TEMPLATE    @DEFAULT_EMAIL_TEMPLATE
    //WITH DEFAULT SIGNATURE    @DEFAULT_EMAIL_SIGNATURE
    //WITH SIGNATURE
    //ATTACH
    //TO
    //CC
    //BCC

    def send(PQL: PQL): Unit = {

        val title = {
            if ($LINK.test(info)) {
                info.takeBefore($LINK)
            }
            else {
                info
            }
        }

        var options = " " + info.takeAfter(title)

        val links = new mutable.ArrayBuffer[(String, String)]()


        $LINK.findAllIn(options)
                    .toArray
                    .reverse
                    .map(l => {
                        val args = options.takeAfter(l)
                        options = options.takeBefore(l)
                        (l.trim().replaceAll(BLANKS, "#").toUpperCase().split("#"), args)
                    })
                    .foreach(vs => {
                        if (vs._1.length > 3) {
                            links += ((vs._1.take(3).mkString("$"), ""))
                            links += ((vs._1.takeRight(vs._1.length - 3).mkString("$"), ""))
                        }
                        else {
                            links += ((vs._1.mkString("$"), vs._2))
                        }
                    })

        val email = new Email(if (title != "") title.$eval(PQL).asText else "")

        for (i <- links.indices) {
            val args = links(i)._2.toArgs(PQL)
            links(i)._1 match {
                case "SET$CONTENT" => if (args.nonEmpty) email.setContent(args.head.asText)
                case "FROM$TEMPLATE" => if (args.nonEmpty) email.setContent(io.qross.pql.PQL.runEmbeddedFile(args.head.asText).toString)
                case "FROM$DEFAULT$TEMPLATE" => email.fromDefaultTemplate()
                case "WITH$SIGNATURE" => if (args.nonEmpty) email.withSignature(io.qross.pql.PQL.runEmbeddedFile(args.head.asText).toString)
                case "WITH$DEFAULT$SIGNATURE" => email.withDefaultSignature()
                case "REPLACE" =>
                    if (args.size == 1) {
                        email.placeData(args.head.asText)
                    }
                    else if (args.size > 1) {
                        email.placeData(args.head.asText, args(1).asText)
                    }
                case "ATTACH" =>
                    if (args.nonEmpty) {
                        email.attach(args.map(_.asText): _*)
                    }
                case "TO" =>
                    if (args.nonEmpty) {
                        email.to(args.map(_.asText).mkString(","))
                    }
                case "CC" =>
                    if (args.nonEmpty) {
                        email.cc(args.map(_.asText).mkString(","))
                    }
                case "BCC" =>
                    if (args.nonEmpty) {
                        email.bcc(args.map(_.asText).mkString(","))
                    }
            }
        }

        email.send()
    }
}
