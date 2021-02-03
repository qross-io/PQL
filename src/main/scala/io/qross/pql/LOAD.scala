package io.qross.pql

import io.qross.pql.Solver._
import io.qross.setting.{Config, Properties}

object LOAD {
    def parse(sentence: String, PQL: PQL): Unit = {
        PQL.PARSING.head.addStatement(new Statement("LOAD", sentence, new LOAD(sentence)))
    }
}

class LOAD(val sentence: String) {
    def execute(PQL: PQL): Unit = {
        val plan = Syntax("LOAD").plan(sentence.$restore(PQL).drop(4).trim())
        val path = plan .headArgs
        plan.head match {
            case "PROPERTIES" => Properties.loadLocalFile(path, Config.Properties)
            case "YAML" | "YML" => Properties.loadLocalFile(path, Config.Yaml)
            case "JSON CONFIG" => Properties.loadLocalFile(path, Config.Json)
            case "PROPERTIES FROM NACOS" => Properties.loadNacosConfig(path, Config.Properties)
            case "YAML FROM NACOS" | "YML FROM NACOS" => Properties.loadNacosConfig(path, Config.Yaml)
            case "JSON CONFIG FROM NACOS" => Properties.loadNacosConfig(path, Config.Json)
            case "PROPERTIES FROM URL" => Properties.loadUrlConfig(path, Config.Properties)
            case "YAML FROM URL" | "YML FROM URL" => Properties.loadUrlConfig(path, Config.Yaml)
            case "JSON CONFIG FROM URL" => Properties.loadUrlConfig(path, Config.Json)
            case _ =>
        }

        if (PQL.dh.debugging) {

        }
    }
}
