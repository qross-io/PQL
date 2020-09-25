package io.qross.fs

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, SimpleFileVisitor, StandardCopyOption}

import io.qross.core.{DataHub, DataRow, DataType}
import io.qross.ext.TypeExt._
import io.qross.setting.Global
import io.qross.time.DateTime
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

object Path {

    implicit class DataHub$Path(val dh: DataHub) {
        def deleteFile(fileName: String): DataHub = {
            fileName.delete()
            dh
        }
    }

    implicit class PathExt(path: String) {

        def toPath: String = {
            path.replace("%QROSS_HOME", Global.QROSS_HOME)
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

        def renameTo(newName: String): Boolean = {
            val file = new File(path.locate())
            if (file.exists()) {
                val targetFile = {
                    if (newName.contains("/") || newName.contains("\\")) {
                        new File(newName)
                    }
                    else {
                        new File(file.getParent + "/" + newName)
                    }
                }

                if (!targetFile.getParentFile.exists()) {
                    targetFile.getParentFile.mkdirs()
                }

                if (targetFile.exists()) {
                    false
                }
                else {
                    Files.move(file.toPath, targetFile.toPath)
                    true
                }
            }
            else {
                false
            }
        }

        //可以指定另一个已经存在的目录或完整的文件路径，如果是一个不存在的目录，会将目录命名为文件名
        def copyTo(target: String, replaceExisting: Boolean = true): Boolean = {
            val file = new File(path.locate())
            if (file.exists()) {
                val targetFile = new File(target.locate())
                if (file.isDirectory) {
                    //文件夹对文件夹拷贝
                    Files.walkFileTree(file.toPath, new CopyDirVisitor(file.toPath, targetFile.toPath, replaceExisting))
                    false
                }
                else {
                    //单文件复制可以只指定目录名，也可以指定另一个文件名
                    val newFile = {
                        if (targetFile.isDirectory) {
                            new File(targetFile.getAbsoluteFile + "/" + file.getName)
                        }
                        else {
                            targetFile
                        }
                    }
                    if (newFile.exists()) {
                        if (replaceExisting) {
                            Files.copy(file.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
                            true
                        }
                        else {
                            false
                        }
                    }
                    else {
                        Files.copy(file.toPath, newFile.toPath)
                        true
                    }
                }
            }
            else {
                false
            }
        }

        def moveTo(target: String, replaceExisting: Boolean = true): Boolean = {
            val file = new File(path.locate())
            if (file.exists()) {
                val targetFile = new File(target.locate())
                if (file.isDirectory) {
                    //文件夹对文件夹拷贝
                    Files.walkFileTree(file.toPath, new MoveDirVisitor(file.toPath, targetFile.toPath, replaceExisting))
                    false
                }
                else {
                    //单文件移动可以只指定目录名，也可以指定另一个文件名
                    val newFile = {
                        if (targetFile.isDirectory) {
                            new File(targetFile.getAbsoluteFile + "/" + file.getName)
                        }
                        else {
                            targetFile
                        }
                    }
                    if (newFile.exists()) {
                        if (replaceExisting) {
                            Files.move(file.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
                            true
                        }
                        else {
                            false
                        }
                    }
                    else {
                        Files.move(file.toPath, newFile.toPath)
                        true
                    }
                }
            }
            else {
                false
            }
        }

        //这种方式会出错，但是不影响功能
        def download(): Boolean = {
            val file = new File(path.locate())
            val attributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]

            // 如果文件存在，则进行下载
            if (file.exists && attributes != null) {
                try {
                    val response = attributes.getResponse
                    response.setHeader("content-type", "application/octet-stream")
                    response.setContentType("application/force-download")
                    // 下载文件能正常显示中文
                    response.setHeader("Content-Disposition", "attachment;filename=" + java.net.URLEncoder.encode(file.getName, Global.CHARSET))
                    response.flushBuffer()
                    // 实现文件下载
                    val buffer = new Array[Byte](1024)
                    val fis = new java.io.FileInputStream(file)
                    val bis = new java.io.BufferedInputStream(fis)

                    val os = response.getOutputStream
                    var i = bis.read(buffer)
                    while (i != -1) {
                        os.write(buffer, 0, i)
                        i = bis.read(buffer)
                    }

                    bis.close()
                    fis.close()
                }
                catch {
                    case e: Exception => println(e.getMessage)
                }

                true
            }
            else {
                false
            }
        }

        //这种方法会用浏览器打开而不是下载
//        def download(): org.springframework.core.io.Resource = {
//            val filePath = new File(path.locate()).toPath
//            val resource = new org.springframework.core.io.UrlResource(filePath.toUri)
//            if (resource.exists) {
//                resource
//            }
//            else {
//                throw new java.net.MalformedURLException("File not found " + path)
//            }
//        }

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

class CopyDirVisitor(val srcPath: java.nio.file.Path, val targetPath: java.nio.file.Path, val replaceExisting: Boolean) extends SimpleFileVisitor[java.nio.file.Path] {
    override def preVisitDirectory(dir: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
        val storePath = targetPath.resolve(srcPath.relativize(dir))
        if (!Files.exists(storePath)) {
            Files.createDirectory(storePath)
        }
        FileVisitResult.CONTINUE
        //super.preVisitDirectory(dir, attrs)
    }

    override def visitFile(file: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (replaceExisting) {
            Files.copy(file, targetPath.resolve(srcPath.relativize(file)), StandardCopyOption.REPLACE_EXISTING)
        }
        else {
            Files.copy(file, targetPath.resolve(srcPath.relativize(file)))
        }
        FileVisitResult.CONTINUE
        //super.visitFile(file, attrs)
        //FileVisitResult还有其他选项
    }

    //另有两个可重载的方法
    //FileVisitResult postVisitDirectory(T dir, IOException exc)
    //FileVisitResult visitFileFailed(T file, IOException exc)
}

class MoveDirVisitor(val srcPath: java.nio.file.Path, val targetPath: java.nio.file.Path, val replaceExisting: Boolean) extends SimpleFileVisitor[java.nio.file.Path] {

    override def preVisitDirectory(dir: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
        val storePath = targetPath.resolve(srcPath.relativize(dir))
        if (!Files.exists(storePath)) {
            Files.createDirectory(storePath)
        }
        FileVisitResult.CONTINUE
    }

    override def postVisitDirectory(dir: java.nio.file.Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
    }

    override def visitFile(file: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (replaceExisting) {
            Files.move(file, targetPath.resolve(srcPath.relativize(file)), StandardCopyOption.REPLACE_EXISTING)
        }
        else {
            Files.move(file, targetPath.resolve(srcPath.relativize(file)))
        }
        FileVisitResult.CONTINUE
    }
}