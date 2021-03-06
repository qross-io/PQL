package io.qross.fs

import java.io.FileNotFoundException

import io.qross.core.DataRow
import io.qross.net.Email
import io.qross.ext.Output
import io.qross.setting.BaseClass

import scala.io.{BufferedSource, Source}

object ResourceFile {
    def open(path: String): ResourceFile = {
        new ResourceFile(path)
    }
}

class ResourceFile(path: String) {

    private val source: BufferedSource = Source.fromInputStream(BaseClass.MAIN.getResourceAsStream(path), "UTF-8")

    val ( content: String, exists: Boolean) =
                try {
                    val string = source.mkString
                    source.close()

                    (string, true)
                }
                catch {
                    case _: Exception =>
                        //Output.writeWarning(s"Resource file $path doesn't exist.")
                        ("", false)
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