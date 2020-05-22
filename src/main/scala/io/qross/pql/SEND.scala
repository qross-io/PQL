package io.qross.pql

import io.qross.exception.SQLParseException
import io.qross.ext.Output
import io.qross.net.Email
import io.qross.pql.Patterns._
import io.qross.pql.Solver._
import io.qross.ext.TypeExt._

import scala.collection.mutable

object SEND {

    def parse(sentence: String, PQL: PQL): Unit = {
        $SEND.findFirstIn(sentence) match {
            case Some(caption) => PQL.PARSING.head.addStatement(new Statement("SEND", sentence, new SEND(sentence.takeAfter(caption))))
            case None => throw new SQLParseException("Incorrect SEND sentence: " + sentence)
        }
    }
}

class SEND(val sentence: String) {

    //SET HOST
    //SET PORT
    //FROM
    //SET PASSWORD
    //SET PERSONAL
    //SET CONTENT
    //USE TEMPLATE
    //USE DEFAULT TEMPLATE    @DEFAULT_EMAIL_TEMPLATE
    //WITH DEFAULT SIGNATURE    @DEFAULT_EMAIL_SIGNATURE
    //WITH SIGNATURE
    //PLACE ... AT
    //ATTACH
    //TO
    //CC
    //BCC

    def execute(PQL: PQL): Unit = {

        val plan = Syntax("SEND").plan(sentence.$restore(PQL))

        val email = new Email(plan.headArgs)
        plan.head match {
            case "MAIL" | "EMAIL" =>
                plan.options.foreach {
                    case "SMTP HOST" | "SET SMTP HOST" => email.setSmtpHost(plan.oneArgs("SMTP HOST", "SET SMTP HOST"))
                    case "PORT" | "SET PORT" => email.setSmtpPort(plan.oneArgs("PORT", "SET PORT"))
                    case "FROM" => email.setFrom(plan.oneArgs("FROM"))
                    case "PERSONAL" | "SET PERSONAL" => email.setFromPersonal(plan.oneArgs("PERSONAL", "SET PERSONAL"))
                    case "PASSWORD" | "SET PASSWORD" => email.setFromPassword(plan.oneArgs("PASSWORD", "SET PASSWORD"))
                    case "LANGUAGE" | "SET LANGUAGE" => email.setTemplateLanguage(plan.oneArgs("LANGUAGE", "SET LANGUAGE"))
                    case "CONTENT" | "SET CONTENT" => email.setContent(plan.oneArgs("CONTENT", "SET CONTENT"))
                    case "TEMPLATE" | "USE TEMPLATE" => email.useTemplate(plan.oneArgs("TEMPLATE", "USE TEMPLATE"))
                    case "DEFAULT TEMPLATE" | "USE DEFAULT TEMPLATE" => email.useDefaultTemplate()
                    case "SIGNATURE" | "WITH SIGNATURE" => email.withSignature(plan.oneArgs("SIGNATURE", "WITH SIGNATURE"))
                    case "DEFAULT SIGNATURE" | "WITH DEFAULT SIGNATURE" => email.withDefaultSignature()
                    case "PLACE DATA" | "DATA" => email.placeData(plan.setArgs("PLACE DATA", "DATA"))
                    case "PLACE" => email.place(plan.oneArgs("PLACE"))
                    case "AT" => email.at(plan.oneArgs("AT"))
                    case "ATTACH" => email.attach(plan.listArgs("ATTACH"): _*)
                    case "TO" => email.to(plan.listArgs("TO").mkString(";"))
                    case "CC" => email.cc(plan.listArgs("CC").mkString(";"))
                    case "BCC" => email.bcc(plan.listArgs("BCC").mkString(";"))
                    case _ =>
                }
            case _ =>
        }

        val result = email.send()
        if (PQL.dh.debugging) {
            Output.writeDebugging(result)
        }
    }
}
