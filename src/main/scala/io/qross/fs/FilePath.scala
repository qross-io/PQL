package io.qross.fs

import java.io.File

import io.qross.ext.TypeExt._
import io.qross.setting.Global

object FilePath {

    implicit class PathExt(path: String) {

        def toPath: String = {
            path.replace("\\", "/")
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
                val p = full.takeRightBefore("/") + "/"
                val f = full.takeRightAfter("/")
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
                full.takeRightBefore("/") + "/"
            }
            else {
                ""
            }
        }

        def takeFile: String = {
            val full = path.toPath
            if (full.contains("/")) {
                full.takeRightAfter("/")
            }
            else {
                full
            }
        }

        def locate(): String = {

            var full = path.toPath

            if (!full.startsWith("/") && !full.contains(":/")) {
//                if (Global.QROSS_SYSTEM == "WORKER") {
//                    full = Global.QROSS_WORKER_HOME  + full
//                }
//                else {
//                    full = Global.QROSS_KEEPER_HOME  + full
//                }
                full = Global.QROSS_WORKER_HOME + "temp/" + full
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
