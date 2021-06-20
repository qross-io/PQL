package io.qross.setting

import java.time.ZoneId

import io.qross.ext.TypeExt._
import io.qross.fs.Path._

object Global {

    def QROSS_VERSION: String = Configurations.getOrProperty("QROSS_VERSION", "qross.version")

    def QROSS_SYSTEM: String = Configurations.getOrProperty("QROSS_SYSTEM", "qross.system").toUpperCase() //current system name, worker/keeper/monitor

    def COMPANY_NAME: String = Configurations.getOrProperty("COMPANY_NAME", "company.name")

    def VOYAGER_LANGUAGE: String = Configurations.getOrProperty("VOYAGER_LANGUAGE", "voyager.language", "english")

    def CHARSET: String = Configurations.getOrProperty("CHARSET", "charset").ifNullOrEmpty("UTF-8")

    def TIMEZONE: String = Configurations.getOrProperty("TIMEZONE", "timezone").ifNullOrEmpty(ZoneId.systemDefault().toString)

    def USER_HOME: String = System.getProperty("user.dir").toDir

    def QROSS_HOME: String = Configurations.getOrProperty("QROSS_HOME", "qross.home").ifNullOrEmpty(USER_HOME).toDir.replace("%USER_HOME", USER_HOME).replace("//", "/")

    def PQL: String = Global.JAVA_BIN_HOME + s"java -Dfile.encoding=${Global.CHARSET} -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar "

    //PQL和DataHub调试
    def DEBUG: Boolean = Configurations.getOrProperty("DEBUG", "pql.debug").toBoolean(false)

    def JAVA_BIN_HOME: String = Configurations.getOrProperty("JAVA_BIN_HOME", "java.bin.home")

    def PYTHON2_HOME: String = Configurations.getOrProperty("PYTHON2_HOME", "python2.home")

    def PYTHON3_HOME: String = Configurations.getOrProperty("PYTHON3_HOME", "python3.home")

    def EMAIL_SMTP_HOST: String = Configurations.getOrProperty("EMAIL_SMTP_HOST", "email.smtp.host")

    def EMAIL_SMTP_PORT: String = Configurations.getOrProperty("EMAIL_SMTP_PORT", "email.smtp.port")

    def EMAIL_SENDER_PERSONAL: String = Configurations.getOrProperty("EMAIL_SENDER_PERSONAL", "email.sender.personal")

    def EMAIL_SENDER_ACCOUNT: String = Configurations.getOrProperty("EMAIL_SENDER_ACCOUNT", "email.sender.account")

    def EMAIL_SENDER_PASSWORD: String = Configurations.getOrProperty("EMAIL_SENDER_PASSWORD", "email.sender.password")

    def EMAIL_SENDER_ACCOUNT_AVAILABLE: Boolean = EMAIL_SMTP_HOST != "" && EMAIL_SMTP_HOST != "smtp.domain.com" && EMAIL_SENDER_ACCOUNT != "" && EMAIL_SENDER_ACCOUNT != "user@domain.com"

    //for Keeper & Master
    def KEEPER_HTTP_ADDRESS: String = {
        var address = Configurations.getOrProperty("KEEPER_HTTP_ADDRESS", "keeper.http.address")
        if (address == "") {
            address = Environment.localHostAddress
            Configurations.set("KEEPER_HTTP_ADDRESS", address)
        }
        address
    }
    def KEEPER_HTTP_PORT: Int = Configurations.getOrProperty("KEEPER_HTTP_PORT", "keeper.http.port", "7700").toInt

    def EMAIL_NOTIFICATION: Boolean = Configurations.getOrProperty("EMAIL_NOTIFICATION", "email.notification").toBoolean(false)
}
