package io.qross.setting

import java.io.{BufferedReader, InputStreamReader}

import io.qross.fs.ResourceDir
import io.qross.ext.TypeExt._
import io.qross.net.Cookies

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.matching.Regex

object Language {

    val include: Regex = """(?i)<#\s*include\s+language=["'](.+?)["']\s*/>""".r
    val holder: Regex = """(?i)#\s*([a-z0-9-]+(\.[a-z0-9-]+)*)\s*#""".r

    private val labels: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    private val props = new java.util.Properties()

    //从languages/english/*.lang加载
    def loadAll(): Unit = {
        ResourceDir.listFiles("/languages")
            .foreach(path => {
                if (path.endsWith(".lang")) {
                    try {
                        props.load(new BufferedReader(new InputStreamReader(BaseClass.MAIN.getResourceAsStream(path))))
                    }
                    catch {
                        case e : Exception => e.printStackTrace()
                    }

                    val module = path.takeAfter("/languages/").dropRight(4).replace("/", ".")
                    props.forEach((key, value) => {
                        labels.put((module + key.toString).toLowerCase(), value.toString)
                    })
                    props.clear()
                }
            })
    }

    def nonEmpty: Boolean = {
        labels.nonEmpty
    }

    def name: String = {
        val language = Cookies.get("language")
        if (language == null) {
            Configurations.getOrProperty("VOYAGER_LANGUAGE", "voyager.language", "english")
        }
        else {
            language
        }
    }

    def verify(languageName: String): String = {
        languageName.toLowerCase match {
            case "cn" | "中文" | "简体中文" | "chinese" => "chinese"
            case "en" | "english" => "english"
            case _ => Language.name
        }
    }

    def get(label: String): String = {
        props.getProperty(label, "NULL")
    }

    def get(modules: String, holder: String): String = {
        get(Language.name, modules, holder)
    }

    def get(language: String, modules: String, holder: String): String = {

        if (labels.isEmpty) {
            loadAll()
        }

        var text: String = null
        val label = (language + "." + holder).toLowerCase()
        if (labels.contains(label)) {
            text = labels(label)
        }
        else {
            breakable {
                for (module <- modules.split(",")) {
                    val label = (language + "." + module.trim() + "." + holder).replace("..", ".").toLowerCase()
                    if (labels.contains(label)) {
                        text = labels(label)
                        break
                    }
                }
            }
        }

        text
    }
}

