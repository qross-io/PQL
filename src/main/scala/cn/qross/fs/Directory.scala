package cn.qross.fs

import java.io.{File, FileFilter}

import cn.qross.ext.TypeExt._

import scala.util.matching.Regex
import cn.qross.fs.Path._

object Directory {

    def listFiles(path: String): Array[File] = {
        listFiles(path, recursive = false)
    }

    def listFiles(path: String, recursive: Boolean): Array[File] = {
        val dir = new File(path.toPath)
        if (dir.exists()) {
            if (dir.isDirectory) {
                if (!recursive) {
                    dir.listFiles(
                        new FileFilter() {
                            override def accept(f: File): Boolean = f.isFile
                        })
                }
                else {
                    dir.listFiles().flatMap(f => {
                        if (f.isFile) {
                            Array[File](f)
                        }
                        else {
                            listFiles(f.getPath, recursive = true)
                        }
                    })
                }
            }
            else {
                Array[File](dir)
            }
        }
        else {
            Array[File]()
        }
    }

    def listFiles(path: String, filter: String): Array[File] = {
        listFiles(path, filter, recursive = false)
    }

    def listFiles(path: String, filter: String, recursive: Boolean): Array[File] = {
        listFiles(path, filter.replace("?", "[\\s\\S]")
                            .replace("*", "[\\s\\S]*")
                            .replace(".", "\\.")
                            .r, recursive)
    }

    def listFiles(path: String, filter: Regex): Array[File] = {
        listFiles(path, filter, recursive = false)
    }

    def listFiles(path: String, filter: Regex, recursive: Boolean): Array[File] = {
        val dir = new File(path.toPath)
        if (dir.exists()) {
            if (dir.isDirectory) {
                if (!recursive) {
                    dir.listFiles(
                        new FileFilter {
                            override def accept(f: File): Boolean = {
                                f.isFile && filter.test(f.getPath)
                            }
                        }
                    )
                }
                else {
                    dir.listFiles().flatMap(f => {
                        if (f.isFile) {
                            if (filter.test(f.getPath)) {
                                Array[File](f)
                            }
                            else {
                                new Array[File](0)
                            }
                        }
                        else {
                            listFiles(f.getPath, filter, recursive = true)
                        }
                    })
                }
            }
            else if (filter.test(path)) {
                Array[File](dir)
            }
            else {
                new Array[File](0)
            }
        }
        else {
            new Array[File](0)
        }
    }

    def list(path: String): Array[File] = {
        if (path.contains("*") || path.contains("?")) {
            val (dir, filter) = {
                if (path.contains("/")) {
                    (path.takeBeforeLast("/"), path.takeAfterLast("/"))
                }
                else if (path.contains("\\")) {
                    (path.takeBeforeLast("\\"), path.takeAfterLast("\\"))
                }
                else {
                    (path, "*")
                }
            }

            Directory.listFiles(dir, filter)
        }
        else {
            val dir = new File(path.toPath)
            if (dir.exists()) {
                dir.listFiles()
            }
            else {
                Array[File]()
            }
        }
    }

    def listDirs(path: String): Array[File] = {
        val dir = new File(path.toPath)
        if (dir.exists()) {
            if (dir.isDirectory) {
                dir.listFiles(
                    new FileFilter() {
                        override def accept(f: File): Boolean = f.isDirectory
                    })
            }
            else {
                Array[File]()
            }
        }
        else {
            Array[File]()
        }
    }

    def spaceUsage(path: String): Long = {
        val dir = new File(path)
        if (dir.exists()) {
            spaceUsage(dir)
        }
        else {
            0
        }
    }

    def spaceUsage(dir: File): Long = {
        var size = 0L
        if (dir.isDirectory) {
            for (file <- dir.listFiles()) {
                if (file.isDirectory) {
                    size += spaceUsage(file.getPath)
                }
                else {
                    size += file.length()
                }
            }
        }
        else {
            size += dir.length()
        }

        size
    }

    def exists(path: String): Boolean = {
        val dir = new File(path)
        dir.exists && dir.isDirectory
    }

    def create(path: String): Boolean = {
        val dir = new File(path)
        if (!dir.exists) {
            dir.mkdirs
        }
        else {
            false
        }
    }

    def clear(path: String): Unit = {
        val dir = new File(path)
        if (dir.isDirectory) {
            for (f <- dir.listFiles) {
                if (f.isDirectory) {
                    clear(f.getPath)
                }
                else {
                    f.delete()
                }
            }
        }
        else {
            dir.delete
        }
    }

    def delete(path: String): Boolean = {
        clear(path)
        val dir = new File(path)
        dir.delete()
    }
}
