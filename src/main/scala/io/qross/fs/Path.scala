package io.qross.fs

import java.io.File

import io.qross.ext.TypeExt._
import io.qross.setting.Global

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
            """(?i)^([a-z]+:/|/)?([a-z0-9$_-]/)*""".r.test(path.toDir)
        }

        def isFile: Boolean = {
            val full = path.toPath
            if (full.contains("/")) {
                val p = full.takeBeforeLast("/") + "/"
                val f = full.takeAfterLast("/")
                p.isDir && f.isFile
            }
            else {
                """^[a-zA-Z0-9\$_-]+\.[a-z0-9]+$""".r.test(full)
            }
        }

        def isFileExists: Boolean = {
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
    }
}
