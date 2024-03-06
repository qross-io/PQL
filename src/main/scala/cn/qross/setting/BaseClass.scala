package cn.qross.setting

import scala.util.control.Breaks._
import cn.qross.ext.TypeExt._

object BaseClass {

    val MAIN: Class[_] = bubble

    def bubble: Class[_] = {
        val classes = Thread.currentThread().getStackTrace
        var className = "cn.qross.setting.BaseClass"
        var found = false
        breakable {
            for (stack <- classes) {
                if (!found && stack.getClassName.startsWith("cn.qross")) {
                    found = true
                }

                if (found) {
                    className = stack.getClassName
                    if (!stack.getClassName.$startsWith(
                        "cn.qross.app",
                                "cn.qross.core",
                                "cn.qross.ext",
                                "cn.qross.exception",
                                "cn.qross.fql",
                                "cn.qross.fs",
                                "cn.qross.jdbc",
                                "cn.qross.look",
                                "cn.qross.net",
                                "cn.qross.pql",
                                "cn.qross.script",
                                "cn.qross.security",
                                "cn.qross.setting",
                                "cn.qross.test",
                                "cn.qross.thread",
                                "cn.qross.time"
                             )) {
                        break
                    }
                }
            }
        }

        Class.forName(className)
    }
}
