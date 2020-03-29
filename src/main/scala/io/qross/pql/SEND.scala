package io.qross.pql

import io.qross.ext.Output
import io.qross.net.Email
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

import scala.collection.mutable

object SEND {

    def parse(sentence: String, PQL: PQL): Unit = {
        if ($SEND$MAIL.test(sentence)) {
            PQL.PARSING.head.addStatement(new Statement("SEND", sentence, new SEND(sentence.takeAfter($SEND$MAIL))))
        }
        else {
            throw new SQLParseException("Incorrect SEND MAIL sentence: " + sentence)
        }
    }
}

class SEND(val info: String) {

    //CONTENT
    //FROM TEMPLATE
    //FROM DEFAULT TEMPLATE    @DEFAULT_EMAIL_TEMPLATE
    //WITH DEFAULT SIGNATURE    @DEFAULT_EMAIL_SIGNATURE
    //WITH SIGNATURE
    //PLACE
    //AT
    //ATTACH
    //TO
    //CC
    //BCC

    def execute(PQL: PQL): Unit = {

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
                        val arg = options.takeAfter(l).trim()
                        options = options.takeBefore(l)
                        (l.trim().replaceAll(BLANKS, "#").toUpperCase().split("#"), arg)
                    })
                    .reverse
                    .foreach(vs => {
                        //检查超过3个单词的link
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
            val arg = links(i)._2.$eval(PQL)
            links(i)._1 match {
                case "CONTENT" | "SET$CONTENT" => email.setContent(arg.asText)
                case "FROM$TEMPLATE" => email.fromTemplate(arg.asText)
                case "FROM$DEFAULT$TEMPLATE" => email.fromDefaultTemplate()
                case "WITH$SIGNATURE" => email.withSignature(arg.asText)
                case "WITH$DEFAULT$SIGNATURE" => email.withDefaultSignature()
                case "PLACE$DATA" =>
                    if (arg.isRow) {
                        email.placeData(arg.asRow)
                    }
                    else {
                        email.placeData(arg.asText)
                    }
                case "PLACE" => email.place(arg.asText)
                case "AT" => email.at(arg.asText)
                case "ATTACH" =>
                    if (arg.isJavaList) {
                        email.attach(arg.asList[String]: _*)
                    }
                    else {
                        email.attach(arg.asText.split(",|;"): _*)
                    }
                case "TO" => email.to(arg.asText)
                case "CC" => email.cc(arg.asText)
                case "BCC" => email.bcc(arg.asText)
                case _ =>
            }
        }

        val result = email.send()

        if (PQL.dh.debugging) {
            Output.writeDebugging(result)
        }
    }
}
