package io.qross.script

import io.qross.core.DataCell
import io.qross.exception.SQLExecuteException
import io.qross.fs.FileWriter
import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}
import io.qross.script.Shell._
import io.qross.fs.Path._
import io.qross.setting.Global
import io.qross.ext.TypeExt._
import io.qross.time.DateTime

object Script {

    def runShell(shell: String): String = {
        if (shell.contains("\n")) {
            //多行shell保存为文件
            val file = Global.QROSS_HOME + "shell/" +  DateTime.now.getString("yyyyMMddHHmmss") + "_" + "abcdefghijklmnopqrstuvwxyz0123456789".shuffle(7)  + ".sh"
            new FileWriter(file).write(shell).close()
            s"chmod +x $file".bash()
            val result = file.knock()
            file.delete()
            result
        }
        else {
            shell.knock()
        }
    }

    def runPython(python: String): String = {
        val file = Global.QROSS_HOME + "python/" + DateTime.now.getString("yyyyMMddHHmmss") + "_" + "abcdefghijklmnopqrstuvwxyz0123456789".shuffle(7)  + ".sh"
        new FileWriter(file).write(python).close()
        val result = (Global.PYTHON3_HOME + "python3 " + file).knock()
        file.delete()
        result
    }

    //运行javascript表达式
    def evalJavascript(script: String): Any = {
        val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
        try {
            jse.eval(script)
        }
        catch {
            case e: ScriptException =>
                e.printStackTrace()
                throw new SQLExecuteException("Can't calculate expression: " + script)
        }
    }

    //执行javascript并返回值, 最后一条语句需以return结尾
    def runJavascript(script: String): DataCell = {
        val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
        try {
            DataCell(jse.eval(s"""(function(){ $script })()"""))
        }
        catch {
            case e: ScriptException =>
                e.printStackTrace()
                throw new SQLExecuteException("Can't caculate expression: " + script)
        }
    }
}


//Script
