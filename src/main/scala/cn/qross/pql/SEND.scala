package cn.qross.pql

import cn.qross.core.{DataCell, DataRow, DataType}
import cn.qross.ext.Output
import cn.qross.net.Email
import cn.qross.pql.Solver._

object SEND {

    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("SEND", sentence, new SEND(sentence)))
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
    //ATTACH
    //TO
    //CC
    //BCC

    def evaluate(PQL: PQL, express: Int = Solver.FULL): DataCell = {
        sentence.$process(PQL, express, body => {
            val plan = Syntax("SEND").plan(body.drop(4).trim())

            val email = new Email(plan.headArgs)
            plan.head match {
                case "MAIL" | "EMAIL" =>
                    plan.options.foreach {
                        case "SMTP HOST" | "SET SMTP HOST" => email.setSmtpHost(plan.oneArgs("SMTP HOST", "SET SMTP HOST"))
                        case "PORT" | "SET PORT" => email.setSmtpPort(plan.oneArgs("PORT", "SET PORT"))
                        case "FROM" => email.setFrom(plan.oneArgs("FROM"))
                        case "PERSONAL" | "SET PERSONAL" => email.setFromPersonal(plan.oneArgs("PERSONAL", "SET PERSONAL"))
                        case "PASSWORD" | "SET PASSWORD" => email.setFromPassword(plan.oneArgs("PASSWORD", "SET PASSWORD"))
                        case "CONTENT" | "SET CONTENT" => email.setContent(plan.oneArgs("CONTENT", "SET CONTENT"))
                        case "TEMPLATE" | "USE TEMPLATE" => email.useTemplate(plan.oneArgs("TEMPLATE", "USE TEMPLATE"))
                        case "DEFAULT TEMPLATE" | "USE DEFAULT TEMPLATE" => email.useDefaultTemplate()
                        case "SIGNATURE" | "WITH SIGNATURE" => email.withSignature(plan.oneArgs("SIGNATURE", "WITH SIGNATURE"))
                        case "DEFAULT SIGNATURE" | "WITH DEFAULT SIGNATURE" => email.withDefaultSignature()
                        case "PLACE DATA" | "DATA" => email.placeData(plan.mapArgs("PLACE DATA", "DATA"))
                        case "ATTACH" => email.attach(plan.listArgs("ATTACH"): _*)
                        case "TO" => email.to(plan.listArgs("TO").mkString(";"))
                        case "CC" => email.cc(plan.listArgs("CC").mkString(";"))
                        case "BCC" => email.bcc(plan.listArgs("BCC").mkString(";"))
                        case _ =>
                    }
                case _ =>
            }

            DataCell(email.send(), DataType.ROW)
        })
    }

    def execute(PQL: PQL): Unit = {
        val result = evaluate(PQL).value
        PQL.WORKING += result

        if (PQL.dh.debugging) {
            val row = result.asInstanceOf[DataRow]
            Output.writeDebugging(row.getString("message"))
            if (row.contains("logs")) {
                row.getCell("logs").asJavaList.forEach(line => {
                    Output.writeDebugging(line)
                })
            }
        }
    }
}
