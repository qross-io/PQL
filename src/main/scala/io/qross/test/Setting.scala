package io.qross.test

import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.Configurations

object Setting {

    def load(): Unit = {
        if (JDBC.hasQrossSystem) {
            DataSource.QROSS.queryDataTable("SELECT conf_key, conf_value FROM qross_conf WHERE belong_to='Master'")
                .foreach(row => {
                    Configurations.set(row.getString("conf_key"), row.getString("conf_value"))
                }).clear()
        }
    }

    //for Master
    def MASTER_LOGO_HTML: String = Configurations.getOrElse("MASTER_LOGO_HTML", """<span class="logo"><span style="color: var(--lighter); text-shadow: 1px 0px 0px #999999; ">io</span><span style="color: #FFCC33; text-shadow: 1px 0px 0px #999999; ">.</span><span style="color: #FFFFFF; text-shadow: 1px 0px 0px var(--lighter); ">Qross</span></span>""").asInstanceOf[String]

    def DEFAULT_SECURITY_AUTHENTICATION: String = Configurations.getOrElse("DEFAULT_SECURITY_AUTHENTICATION", "standard").asInstanceOf[String] //standard/sso/ldap

    def LDAP_URL: String = Configurations.get("LDAP_URL")

    def LDAP_DIR: String = Configurations.get("LDAP_DIR")
}
