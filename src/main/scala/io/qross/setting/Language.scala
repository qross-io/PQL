package io.qross.setting

import java.io.{BufferedReader, InputStreamReader}

import io.qross.fs.ResourceDir
import io.qross.ext.TypeExt._
import io.qross.net.Cookies

import scala.collection.mutable
import scala.util.control.Breaks._

object Language {

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

    def name: String = {
        val language = Cookies.get("language")
        if (language == null) {
            Configurations.getPropertyOrElse("voyager.language", "VOYAGER_LANGUAGE", "chinese")
        }
        else {
            language
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

