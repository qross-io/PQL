package io.qross.script

import java.util

import io.qross.core.{DataHub, DataRow, DataType}
import io.qross.time.Timer
import io.qross.ext.TypeExt._

import scala.collection.mutable
import scala.sys.process._
import scala.sys.process.ProcessLogger

object Shell {

    //查找进程ID
    def ps$ef(grep: String): List[Int] = {
        val ids = new mutable.ListBuffer[Int]()
        val logger = ProcessLogger(out => {
            ids += out.split("\\s+")(1).toInt
        })

        s"""ps -ef | grep "$grep" | grep -v grep""".shell.!(logger)

        ids.toList
    }

    //根据父进程ID 查找子进程
    def ps$ef(proc: Int): List[Int] = {

        val ids = new mutable.ListBuffer[Int]()
        val logger = ProcessLogger(out => {
            val part = out.split("\\s+")
            if (part(2).toInt == proc) {
                ids += part(1).toInt
            }
        })

        s"""ps -ef | grep " $proc " | grep -v grep""".shell.!(logger)

        ids.toList
    }

    //查找并杀死进程
    //返回exitValue
    def kill(grep: String): Int = {
        s"""ps -ef | grep "$grep" | grep -v grep | awk '{print $$2}' | xargs kill -9""".shell.!
    }

    def kill(id: Int): Int = {
        s"kill -9 $id".!
    }

    //杀掉进程及其子进程, 返回杀死的进程数量
    def destroy(grep: String): Int = {
        var exit = 0
        ps$ef(grep).foreach(id => {
                ps$ef(id).foreach(s => {
                    exit += kill(s)
                })
            exit += kill(id)
        })

        exit
    }

    //结束所有子进程, 但保留父进程
    def end(grep: String): Int = {
        var exit = 0
        ps$ef(grep).foreach(id => {
            ps$ef(id).foreach(s => {
                exit += kill(s)
            })
        })

        exit
    }

    implicit class DataHub$Shell(val dh: DataHub) {
        def runCommand(commandText: String): DataHub = {
            commandText.bash()
            dh
        }
    }

    implicit class CommandExt(var command: String) {

        //将单行命令转成可执行的shell命令
        //分号 多个命令 转成 ### 连接
        //管道符 转成 #| 连接
        //引号   转成 Seq 参数
        def shell: ProcessBuilder = {
            val chars = new mutable.ListBuffer[String]()
            //是否包含引号
            val quote = {
                if (command.contains("'") || command.contains("\"")) {
                    command = command.pickChars(chars)
                    true
                }
                else {
                    false
                }
            }

            val list = new mutable.ArrayBuffer[String]()
            if (command.contains(";")) {
                list ++= command.split(";")
            }
            else {
                list += command
            }

            val result: mutable.ArrayBuffer[ProcessBuilder] = {
                if (command.contains("|") && quote) {
                    list.map(cmd => cmd.split("\\|").map(_.trim()))
                        .map(part => {
                            part.map(p => {
                                    if (p.contains("~str[")) {
                                        p.split("\\s+")
                                            .map(s => {
                                                if (s.bracketsWith("~str[", "]")) {
                                                    chars(s.$trim("~str[", "]").toInt).removeQuotes()
                                                }
                                                else {
                                                    s
                                                }
                                            }).toSeq.cat
                                    }
                                    else {
                                        p.cat
                                    }
                                })
                        })
                        .map(pipe => {
                            var sh = pipe(0) #| pipe(1)
                            for (i <- 2 until pipe.length) {
                                sh = sh #| pipe(i)
                            }
                            sh
                        })
                }
                else if (command.contains("|")) {
                    list.map(cmd => cmd.split("\\|").map(_.trim()))
                        .map(pipe => {
                            var sh = pipe(0) #| pipe(1)
                            for (i <- 2 until pipe.length) {
                                sh = sh #| pipe(i)
                            }
                            sh
                        })
                }
                else if (quote) {
                    list.map(_.split("\\s+")
                        .map(s => {
                            if (s.bracketsWith("~str[", "]")) {
                                chars(s.$trim("~str[", "]").toInt).removeQuotes()
                            }
                            else {
                                s
                            }
                        }).toSeq.cat)
                }
                else {
                    list.map(_.cat)
                }
            }

            if (result.length == 1) {
                result(0)
            }
            else {
                // ### 表示依次运行
                var pb = result(0) ### result(1)
                for (i <- 2 until result.length) {
                    pb = pb ### result(i)
                }
                pb
            }
        }

        //运行并返回exitValue
        def bash(): Int = {
            command.shell.!(
                ProcessLogger(out => {
                    println(out)
                }, err => {
                    System.err.println(err)
                })
            )
        }

        //运行并返回输出内容
        def knock(): String = {
            command.shell.!!
        }

        //命令执行模板+仅打印日志
        def run(): DataRow = {
            val logs = new util.ArrayList[String]()
            val errors = new util.ArrayList[String]()
            val process = command.shell.run(ProcessLogger(out => {
                println(out)
                logs.add(out)
            }, err => {
                System.err.println(err)
                logs.add(err)
                errors.add(err)
            }))

            while (process.isAlive()) {
                Timer.sleep(100)
            }

            val exit = new DataRow()
            exit.set("code", process.exitValue(), DataType.INTEGER)
            exit.set("logs", logs, DataType.ARRAY)
            exit.set("errors", errors, DataType.ARRAY)

            exit
        }

        //命令执行+日志和心跳处理
        def run(log: String => Unit, except: String => Unit, tick: () => Unit): Int = {
            val process = command.shell.run(ProcessLogger(out => {
                println(out)
                log(out)
            }, err => {
                System.err.println(err)
                except(err)
            }))

            while (process.isAlive()) {
                Timer.sleep(100)
                tick()
            }

            process.exitValue()
        }
    }
}