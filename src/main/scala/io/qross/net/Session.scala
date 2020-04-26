package io.qross.net

import io.qross.core.{DataCell, DataType}
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

class Session {

    def getCell(name: String): DataCell = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            val value = attributes.getRequest.getSession.getAttribute(name)
            if (value == null) {
                DataCell(null, DataType.TEXT)
            }
            else {
                DataCell(value)
            }
        }
        else {
            DataCell.NOT_FOUND
        }
    }

    def set(): Boolean = {
        val attributes: ServletRequestAttributes = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes]
        if (attributes != null) {
            true
        }
        else {
            false
        }
    }

    override def toString: String = "io.qross.net.Session"
}
