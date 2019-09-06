package io.qross.fs

import java.io.{File, IOException}
import java.util
import java.util.jar.{JarEntry, JarFile}
import java.util.regex.Pattern

import io.qross.setting.BaseClass
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.matching.Regex

object ResourceDir {
    def open(path: String): ResourceDir = {
        new ResourceDir(path)
    }

    //support wildcards
    def listFiles(path: String, filter: String): Array[String] = {
        new ResourceDir(path).listFiles(filter)
    }

    //support regex - for Java
    def listFiles(path: String, filter: Pattern): Array[String] = {
        new ResourceDir(path).listFiles(filter)
    }

    def listFiles(path: String, filter: Regex):  Array[String] = {
        new ResourceDir(path).listFiles(filter)
    }

    def listFiles(path: String): Array[String] = {
        new ResourceDir(path).listFiles("".r)
    }
}

class ResourceDir(path: String) {

    def listFiles(): Array[String] = {
        listFiles("".r)
    }

    def listFiles(filter: String): Array[String] = {
        listFiles(
            filter.replace("?", "[\\s\\S]")
                  .replace("*", "[\\s\\S]*")
                  .replace(".", "\\.")
                  .r)
    }

    def listFiles(filter: Pattern): Array[String] = {
        var pattern = filter.pattern()
        if (!pattern.startsWith("(?i)")) {
            pattern = "(?i)" + pattern
        }
        listFiles(pattern.r)
    }

    def listFiles(filter: Regex): Array[String] = {

        try {
            val root: String = BaseClass.MAIN.getProtectionDomain.getCodeSource.getLocation.getPath
            val localJarFile: JarFile = new JarFile(new File(root))
            val entries: util.Enumeration[JarEntry] = localJarFile.entries
            val files = new mutable.ArrayBuffer[String]()
            while (entries.hasMoreElements) {
                val jarEntry: JarEntry = entries.nextElement
                val name: String = jarEntry.getName
                if (name.startsWith(if (path.startsWith("/")) path.drop(1) else path)) {
                    if (filter.regex != "") {
                        if (filter.test(name)) {
                            files += "/" + name
                        }
                    }
                    else if (!name.endsWith("/")) {
                        files += "/" + name
                    }
                }
            }

            files.toArray
        } catch {
            case _: IOException =>
                val resource = BaseClass.MAIN.getResourceAsStream(path)
                if (resource != null) {
                    val sl = if (path.endsWith("/")) "" else "/"
                    val files = Source.fromInputStream(resource, "UTF-8")
                        .mkString
                        .split("\\s")

                    files.filter(name => filter.test(name))
                        .map(f => path + sl + f.trim()) ++:
                    files.filter(!_.contains("."))
                            .flatMap(dir => ResourceDir.listFiles(path + sl + dir + "/", filter))
                }
                else {
                    new Array[String](0)
                }
        }
    }

    def readAll(filter: String): Array[String] = {
        listFiles(filter).map(file => {
            ResourceFile.open(file).output
        })
    }
}
