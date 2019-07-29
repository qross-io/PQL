package io.qross.fs

import java.io.{File, IOException}
import java.util
import java.util.jar.{JarEntry, JarFile}

import io.qross.setting.BaseClass

import scala.collection.mutable
import scala.io.Source

object ResourceDir {
    def open(path: String): ResourceDir = {
        new ResourceDir(path)
    }
}

class ResourceDir(path: String) {

    def listFiles(ends: String = ""): List[String] = {

        try {
            val root: String = BaseClass.MAIN.getProtectionDomain.getCodeSource.getLocation.getPath
            val localJarFile: JarFile = new JarFile(new File(root))
            val entries: util.Enumeration[JarEntry] = localJarFile.entries
            val files = new mutable.ArrayBuffer[String]()
            while (entries.hasMoreElements) {
                val jarEntry: JarEntry = entries.nextElement
                val name: String = jarEntry.getName
                if (name.startsWith(if (path.startsWith("/")) path.drop(1) else path)) {
                    if (ends != "") {
                        if (name.endsWith(ends)) {
                            files += "/" + name
                        }
                    }
                    else if (!name.endsWith("/")) {
                        files += "/" + name
                    }
                }
            }

            files.toList
        } catch {
            case _: IOException =>
                val resource = BaseClass.MAIN.getResourceAsStream(path)
                if (resource != null) {
                    Source.fromInputStream(resource, "UTF-8")
                            .mkString
                            .split("\\s")
                            .map(f => path + (if (path.endsWith("/")) "" else "/") + f.trim())
                            .toList
                }
                else {
                    List[String]()
                }
        }
    }

    def readAll(ends: String = ""): List[String] = {
        listFiles(ends).map(file => {
            ResourceFile.open(file).output
        })
    }
}
