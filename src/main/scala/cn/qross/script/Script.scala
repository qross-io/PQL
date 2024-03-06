package cn.qross.script

import cn.qross.core.DataCell
import cn.qross.exception.SQLExecuteException
import cn.qross.fs.FileWriter

import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}
import cn.qross.script.Shell._
import cn.qross.fs.Path._
import cn.qross.setting.Global
import cn.qross.ext.TypeExt._
import cn.qross.time.DateTime
import org.graalvm.polyglot.{Context, Value}

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
        //java 将在高版本中移除对javascript引擎的支持
//        val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
//        try {
//            jse.eval(script)
//        }
//        catch {
//            case e: ScriptException =>
//                e.printStackTrace()
//                throw new SQLExecuteException("Can't calculate expression: " + script)
//        }

        val context = Context.newBuilder().allowAllAccess(true).build()
        try {
            context.eval("js", script).as(classOf[Object])
        }
        catch {
            case e: Exception =>
                e.printStackTrace()
                throw new SQLExecuteException("Can't calculate expression: " + script)
        }
        finally {
            if (context != null) context.close()
        }
    }

    //执行javascript并返回值, 最后一条语句需以 return 结尾
    def runJavascript(script: String): DataCell = {

        val context = Context.newBuilder().allowAllAccess(true).build()
        try {
            DataCell(context.eval("js", s"""(function(){ $script })()""").as(classOf[Object]))
        }
        catch {
            case e: Exception =>
                e.printStackTrace()
                throw new SQLExecuteException("Can't calculate expression: " + script)
        }
        finally {
            if (context != null) context.close()
        }
    }
}