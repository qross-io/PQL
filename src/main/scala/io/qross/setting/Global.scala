package io.qross.setting

import io.qross.ext.TypeExt._
import io.qross.fs.Path._

object Global {

    def QROSS_VERSION: String = Configurations.getOrProperty("QROSS_VERSION", "qross.version")

    def QROSS_SYSTEM: String = Configurations.getOrProperty("QROSS_SYSTEM", "qross.system").toUpperCase() //current system name, worker/keeper/monitor

    def COMPANY_NAME: String = Configurations.getOrProperty("COMPANY_NAME", "company.name")

    def CHARSET: String = Configurations.getOrProperty("CHARSET", "charset").ifNullOrEmpty("UTF-8")

    def USER_HOME: String = System.getProperty("user.dir").toDir

    def QROSS_HOME: String = Configurations.getOrProperty("QROSS_HOME", "qross.home").ifNullOrEmpty(USER_HOME).toDir.replace("%USER_HOME", USER_HOME).replace("//", "/")

    def PQL: String = Global.JAVA_BIN_HOME + s"java -jar ${Global.QROSS_HOME}qross-worker-${Global.QROSS_VERSION}.jar "

    def JAVA_BIN_HOME: String = Configurations.getOrProperty("JAVA_BIN_HOME", "java.bin.home")

    def PYTHON2_HOME: String = Configurations.getOrProperty("PYTHON2_HOME", "python2.home")

    def PYTHON3_HOME: String = Configurations.getOrProperty("PYTHON3_HOME", "python3.home")

    def EMAIL_SMTP_HOST: String = Configurations.getOrProperty("EMAIL_SMTP_HOST", "email.smtp.host")

    def EMAIL_SMTP_PORT: String = Configurations.getOrProperty("EMAIL_SMTP_PORT", "email.smtp.port")

    def EMAIL_SENDER_PERSONAL: String = Configurations.getOrProperty("EMAIL_SENDER_PERSONAL", "email.sender.personal")

    def EMAIL_SENDER_ACCOUNT: String = Configurations.getOrProperty("EMAIL_SENDER_ACCOUNT", "email.sender.account")

    def EMAIL_SENDER_PASSWORD: String = Configurations.getOrProperty("EMAIL_SENDER_PASSWORD", "email.sender.password")

    def EMAIL_SSL_AUTH_ENABLED: Boolean = Configurations.getOrProperty("EMAIL_SSL_AUTH_ENABLED", "email.ssl.auth.enabled").toBoolean(false)

    def EMAIL_TEMPLATES_PATH: String = Configurations.getOrProperty("EMAIL_TEMPLATES_PATH", "email.templates.path")

    def EMAIL_DEFAULT_TEMPLATE: String = Configurations.getOrProperty("EMAIL_DEFAULT_TEMPLATE", "email.default.template")

    def EMAIL_DEFAULT_SIGNATURE: String = Configurations.getOrProperty("EMAIL_DEFAULT_SIGNATURE", "email.default.signature")

    def EXCEL_TEMPLATES_PATH: String = Configurations.getOrProperty("EXCEL_TEMPLATES_PATH", "excel.templates.path")

    def EXCEL_DEFAULT_TEMPLATE: String = Configurations.getOrProperty("EXCEL_DEFAULT_TEMPLATE", "excel.default.template")

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

}
