package cn.qross.setting

import java.io.{BufferedReader, InputStreamReader}

import cn.qross.fs.ResourceDir
import cn.qross.ext.TypeExt._
import cn.qross.net.Cookies

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.matching.Regex

object Language {

    val include: Regex = """(?i)<#\s*include\s+language=["'](.+?)["']\s*/>""".r
    val holder: Regex = """(?i)#\s*([a-z0-9-]+(\.[a-z0-9-]+)*)\s*#""".r

    private val labels: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    private val props = new java.util.Properties()

    //从 languages/english/*.lang 加载
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
            Global.VOYAGER_LANGUAGE
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

    def get(holder: String, modules: java.util.List[String]): String = {
        if (labels.isEmpty) {
            loadAll()
        }

        var text: String = null
        val langName = Language.name
        val label = (langName + "." + holder).toLowerCase()
        if (labels.contains(label)) {
            text = labels(label)
        }
        else {
            breakable {
                modules.forEach(module => {
                    val label = (langName + "." + module + "." + holder).toLowerCase()
                    if (labels.contains(label)) {
                        text = labels(label)
                        break
                    }
                })
            }
        }

        text
    }

    def getModuleContent(module: String): java.util.HashMap[String, String] = {
        val languages = new java.util.HashMap[String, String]()
        val language = Language.name
        for ((key, value) <- labels) {
            if (key.startsWith(language + "." + module + ".")) {
                languages.put(key.takeAfterLast("."), value)
            }
        }
        languages
    }
}

