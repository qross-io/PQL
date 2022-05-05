package io.qross.fs

import java.io.InputStream

import io.qross.core.DataRow
import io.qross.ext.TypeExt._
import io.qross.net.Email
import io.qross.setting.BaseClass

import scala.io.{BufferedSource, Source}

object ResourceFile {
    def open(path: String): ResourceFile = {
        new ResourceFile(path)
    }
}

class ResourceFile(path: String) {

    private var stream: InputStream = BaseClass.MAIN.getResourceAsStream(path)
    if (stream == null && !path.startsWith("/templates/") && !path.startsWith("templates/")) {
        stream = BaseClass.MAIN.getResourceAsStream("/templates" + path.prefix("/"))
    }

    val exists: Boolean = stream != null

    val content: String = {
        if (exists) {
            try {
                val source: BufferedSource = Source.fromInputStream(stream, "UTF-8")
                val string = source.mkString
                source.close()

                string
            }
            catch {
                case _: Exception =>
                    //Output.writeWarning(s"Resource file $path doesn't exist.")
                    ""
            }
        }
        else {
            ""
        }
    }

    var output: String = content

    def replace(oldStr: String, newStr: String): ResourceFile = {
        output = output.replace(oldStr, newStr)
        this
    }

    def replaceWith(row: DataRow): ResourceFile = {
        row.foreach((key, value) => {
            output = output.replace("#{" + key + "}", if (value != null) value.toString else "")
        })
        this
    }
    
    def writeEmail(title: String): Email = {
        Email.write(title).setContent(output)
    }
}