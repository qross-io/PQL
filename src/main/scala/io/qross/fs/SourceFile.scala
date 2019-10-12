package io.qross.fs

import java.io.{File, FileNotFoundException}
import io.qross.fs.FilePath._

import scala.io.Source

//源文件类，主要用于配置，用于整个读取

object SourceFile {
    def read(path: String): String = {
        new SourceFile(path).content
    }
}

class SourceFile(val path: String) {

    def content: String = {
        val resource = ResourceFile.open(path)
        if (resource.exists) {
            resource.output
        }
        else {
            val file = new File(path.locate())
            if (file.exists()) {
                //如果不加编码读取不了中文
                Source.fromFile(file, "UTF-8").mkString
            }
            else {
                throw new FileNotFoundException(s"File or Resource file $path doesn't exists.")
            }
        }
    }
}
