package io.qross.pql

import io.qross.time.Timer
import io.qross.script.Shell._

class ParExecutor(commandType: String, command: String, args: String) extends Thread {
    override def run(): Unit = {
        while (!PAR.pipeline.isEmpty) {
            val row = PAR.pipeline.poll()
            if (row != null) {
                if (commandType == "PQL") {
                    PQL.openFile(command).place(row).place(args).run()
                }
                else {
                    (command + " " + row.join()).run()
                }

                Timer.sleep(10)
            }
        }
    }
}
