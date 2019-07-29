package io.qross.setting

import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._

object Global {

    def QROSS_VERSION: String = Configurations.getOrProperty("QROSS_VERSION", "qross.version")

    def QROSS_SYSTEM: String = Configurations.getOrProperty("QROSS_SYSTEM", "qross.system").toUpperCase() //current system name, worker/keeper/monitor

    def COMPANY_NAME: String = Configurations.getOrProperty("COMPANY_NAME", "company.name")

    def CHARSET: String = Configurations.getOrProperty("CHARSET", "charset")

    def USER_HOME: String = System.getProperty("user.dir").toDir

    def QROSS_HOME: String = Configurations.getOrProperty("QROSS_HOME", "qross.home").ifNullOrEmpty(USER_HOME).toDir.replace("%USER_HOME", USER_HOME).replace("//", "/")

    def QROSS_WORKER_HOME: String = Configurations.getOrProperty("QROSS_WORKER_HOME", "qross.worker.home").ifNullOrEmpty(QROSS_HOME + "/worker/").toDir.replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME", USER_HOME).replace("//", "/")

    def QROSS_KEEPER_HOME: String = Configurations.getOrProperty("QROSS_KEEPER_HOME", "qross.keeper.home").ifNullOrEmpty(QROSS_HOME + "/keeper/").toDir.replace("%QROSS_HOME", QROSS_HOME).replace("%USER_HOME", USER_HOME).replace("//", "/")

    def JAVA_BIN_HOME: String = Configurations.getOrProperty("JAVA_BIN_HOME", "java.bin.home")

    def PYTHON2_HOME: String = Configurations.getOrProperty("PYTHON2_HOME", "python2.home")

    def PYTHON3_HOME: String = Configurations.getOrProperty("PYTHON3_HOME", "python3.home")

    def EMAIL_NOTIFICATION: Boolean = Configurations.getOrProperty("EMAIL_NOTIFICATION", "email.notification").toBooleanOrElse(false)

    def EMAIL_SMTP_HOST: String = Configurations.getOrProperty("EMAIL_SMTP_HOST", "email.smtp.host")

    def EMAIL_SMTP_PORT: String = Configurations.getOrProperty("EMAIL_SMTP_PORT", "email.smtp.port")

    def EMAIL_SENDER_PERSONAL: String = Configurations.getOrProperty("EMAIL_SENDER_PERSONAL", "email.sender.personal")

    def EMAIL_SENDER_ACCOUNT: String = Configurations.getOrProperty("EMAIL_SENDER_ACCOUNT", "email.sender.account")

    def EMAIL_SENDER_PASSWORD: String = Configurations.getOrProperty("EMAIL_SENDER_PASSWORD", "email.sender.password")

    def EMAIL_SSL_AUTH_ENABLED: Boolean = Configurations.getOrProperty("EMAIL_SSL_AUTH_ENABLED", "email.ssl.auth.enabled").toBooleanOrElse(false)

    def EMAIL_DEFAULT_TEMPLATE: String = Configurations.getOrProperty("EMAIL_DEFAULT_TEMPLATE", "email.default.template")

    def EMAIL_DEFAULT_SIGNATURE: String = Configurations.getOrProperty("EMAIL_DEFAULT_SIGNATURE", "email.default.signature")

    def HADOOP_AND_HIVE_ENABLED: Boolean = Configurations.getOrProperty("HADOOP_AND_HIVE_ENABLED", "hadoop.and.hive.enabled").toBooleanOrElse(false)

    def LOGS_LEVEL: String = Configurations.getOrProperty("logs.level", "LOGS_LEVEL", "DEBUG").toUpperCase

    def CONCURRENT_BY_CPU_CORES: Int = Configurations.getOrProperty("CONCURRENT_BY_CPU_CORES", "concurrent.by.cpu.cores").ifNullOrEmpty("4").toInt

    def EMAIL_EXCEPTIONS_TO_DEVELOPER: Boolean = Configurations.getOrProperty("EMAIL_EXCEPTIONS_TO_DEVELOPER", "email.exceptions.to.developer").toBooleanOrElse(true)

    def QUIT_ON_NEXT_BEAT: Boolean = Configurations.get("QUIT_ON_NEXT_BEAT").toBooleanOrElse(false)  //for keeper only

    def MASTER_USER_GROUP: String = Configurations.get("MASTER_USER_GROUP")

    def KEEPER_USER_GROUP: String = Configurations.get("KEEPER_USER_GROUP")

    def API_ON_TASK_NEW: String = Configurations.get("API_ON_TASK_NEW")

    def API_ON_TASK_CHECKING_LIMIT: String = Configurations.get("API_ON_TASK_CHECKING_LIMIT")

    def API_ON_TASK_READY: String = Configurations.get("API_ON_TASK_READY")

    def API_ON_TASK_FAILED: String = Configurations.get("API_ON_TASK_FAILED")

    def API_ON_TASK_INCORRECT: String = Configurations.get("API_ON_TASK_INCORRECT")

    def API_ON_TASK_TIMEOUT: String = Configurations.get("API_ON_TASK_TIMEOUT")

    def API_ON_TASK_SUCCESS: String = Configurations.get("API_ON_TASK_SUCCESS")

    def CLEAN_TASK_RECORDS_FREQUENCY: String = Configurations.getOrProperty("CLEAN_TASK_RECORDS_FREQUENCY", "clean.task.records.frequency")

    def BEATS_MAILING_FREQUENCY: String = Configurations.getOrProperty("BEATS_MAILING_FREQUENCY", "beats.mailing_frequency")

    def EXCEL_TEMPLATES_PATH: String = Configurations.getOrProperty("EXCEL_TEMPLATES_PATH", "excel.templates.path")

    def KERBEROS_AUTH: Boolean = Configurations.getOrProperty("KERBEROS_AUTH", "kerberos.auth").toBooleanOrElse(false)

    def KRB_USER_PRINCIPAL: String = Configurations.getOrProperty("KRB_USER_PRINCIPAL", "krb.user.principal")

    def KRB_KEYTAB_PATH: String = Configurations.getOrProperty("KRB_KEYTAB_PATH", "krb.keytab.path")

    def KRB_KRB5CONF_PATH: String = Configurations.getOrProperty("KRB_KRB5CONF_PATH", "krb.krb5conf.path")

    def CORES: Int = Runtime.getRuntime.availableProcessors
}
