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

        def locate(): String = {

            var full = path.toPath

            if (!full.startsWith("/") && !full.contains(":/")) {
                if (Global.QROSS_SYSTEM == "WORKER") {
                    full = Global.QROSS_WORKER_HOME  + full
                }
                else {
                    full = Global.QROSS_KEEPER_HOME  + full
                }
            }

            val parent = new File(full).getParentFile
            if (!parent.exists()) {
                parent.mkdir()
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
