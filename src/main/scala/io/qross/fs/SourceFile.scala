package io.qross.fs

import java.io.{File, FileNotFoundException}

import io.qross.fs.FilePath._
import io.qross.setting.Global

import scala.io.Source

//源文件类，主要用于配置，用于整个读取

object SourceFile {
    def read(path: String): String = {
        new SourceFile(path).content
    }
}

class SourceFile(val path: String) {

    def content: String = {

        val file = new File(path.locate())
        if (file.exists()) {
            //如果不加编码读取不了中文
            Source.fromFile(file, Global.CHARSET).mkString
        }
        else {
            val resource = ResourceFile.open(path)
            if (resource.exists) {
                resource.output
            }
            else if (path.toLowerCase.endsWith(".html") || path.toLowerCase.endsWith(".htm")) {
                //可能是邮件模板
                Source.fromFile(s"%EMAIL_TEMPLATES_PATH$path".toPath, Global.CHARSET).mkString
            }
            else {
                throw new FileNotFoundException(s"File or Resource file $path doesn't exists.")
            }
        }
    }
}
