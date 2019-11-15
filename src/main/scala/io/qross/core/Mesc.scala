package io.qross.core

import io.qross.fs.Path._
import io.qross.ext.TypeExt._

object Mesc {
    implicit class DataHub$Ext(val dh: DataHub) {
        // ---------- other ----------

        def deleteFile(fileName: String): DataHub = {
            fileName.delete()
            dh
        }

        def runCommand(commandText: String): DataHub = {
            commandText.bash()
            dh
        }
    }
}
