package io.qross.setting

import scala.util.control.Breaks._
import io.qross.ext.TypeExt._

object BaseClass {

    val MAIN: Class[_] = bubble

    def bubble: Class[_] = {
        val classes = Thread.currentThread().getStackTrace
        var className = "io.qross.setting.BaseClass"
        var found = false
        breakable {
            for (stack <- classes) {
                if (!found && stack.getClassName.startsWith("io.qross")) {
                    found = true
                }

                if (found) {
                    className = stack.getClassName
                    if (!stack.getClassName.$startsWith(
                        "io.qross.app",
                                 "io.qross.setting",
                                 "io.qross.fs",
                                 "io.qross.jdbc",
                                 "io.qross.core",
                                 "io.qross.ext",
                                 "io.qross.net",
                                 "io.qross.pql",
                                 "io.qross.security",
                                 "io.qross.time",
                                 "io.qross.thread",
                                 "io.qross.test",
                                 "io.qross.fql")) {
                        break
                    }
                }
            }
        }

        Class.forName(className)
    }
}
