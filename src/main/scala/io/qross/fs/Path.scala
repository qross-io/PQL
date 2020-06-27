package io.qross.fs

import java.io.File

import io.qross.core.{DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.setting.Global
import io.qross.time.DateTime

object Path {

    implicit class PathExt(path: String) {

        def toPath: String = {
            path.replace("%QROSS_HOME", Global.QROSS_HOME)
                .replace("%EMAIL_TEMPLATES_PATH", Global.EMAIL_TEMPLATES_PATH)
                .replace("%EXCEL_TEMPLATES_PATH", Global.EXCEL_TEMPLATES_PATH)
                .replace("\\", "/")
                .replace("//", "/")
        }

        def toDir: String = {
            var dir = path.replace("\\", "/")
            if (!path.endsWith("/")) {
                dir += "/"
            }
            dir
        }

        def isDir: Boolean = {
            val dir = new File(path.toPath)
            dir.exists() && dir.isDirectory
        }

        def fileExists: Boolean = {
            new File(path.locate()).exists()
        }

        def takeDir: String = {
            val full = path.toPath
            if (full.contains("/")) {
                full.takeBeforeLast("/") + "/"
            }
            else {
                ""
            }
        }

        def takeFile: String = {
            val full = path.toPath
            if (full.contains("/")) {
                full.takeAfterLast("/")
            }
            else {
                full
            }
        }

        def locate(): String = {

            var full = path.toPath

            if (!full.startsWith("/") && !full.contains(":/")) {
                full = Global.QROSS_HOME + "temp/" + full
            }

            val parent = new File(full).getParentFile
            if (!parent.exists()) {
                parent.mkdirs()
            }

            full
        }

        def delete(): Boolean = {
            val file = new File(path.locate())
            if (file.exists() && file.isFile) {
                file.delete()
            }
            else {
                false
            }
        }

        def fileLength(): Long = {
            val file = new File(path.locate())
            if (file.exists() && file.isFile) {
                file.length()
            }
            else {
                -1
            }
        }

        def makeFile(): Boolean = {
            val file = new File(path.locate())
            if (file.exists()) {
                false
            }
            else {
                file.createNewFile()
            }
        }

        def fileInfo: DataRow = {
            new File(path.locate()).info
        }

        def dirInfo: DataRow = {
            new File(path).info
        }
    }

    implicit class PathFileExt(path: File) {
        def info: DataRow = {
            val info = new DataRow()
            if (path.exists()) {
                val fileName = path.getName
                info.set("path", path.getAbsolutePath)
                info.set("exists", true, DataType.BOOLEAN)
                info.set("name", fileName, DataType.TEXT)
                info.set("extension",
                        if (path.isFile) {
                            if (fileName.contains(".")) fileName.takeAfter(".").toLowerCase() else ""
                        }
                        else {
                            "<DIR>"
                        }, DataType.TEXT)
                info.set("last_modified", new DateTime(path.lastModified()), DataType.DATETIME)
                info.set("length",
                    if (path.isFile) {
                        path.length()
                    }
                    else {
                        Directory.spaceUsage(path)
                    }, DataType.INTEGER)
                info.set("size",
                    if (path.isFile) {
                        path.length()
                    }
                    else {
                        Directory.spaceUsage(path)
                    }.toHumanized, DataType.TEXT)
                info.set("parent", path.getParent, DataType.TEXT)
            }
            else {
                info.set("path", path.getAbsolutePath)
                info.set("exists", false, DataType.BOOLEAN)
            }

            info
        }
    }
}
