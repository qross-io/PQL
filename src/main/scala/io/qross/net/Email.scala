package io.qross.net

import io.qross.core._
import io.qross.exception.{EmailInvalidSenderException, ExtensionNotFoundException}
import io.qross.ext.TypeExt._
import io.qross.fs.Path._
import io.qross.fs.SourceFile
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.Solver._
import io.qross.pql.PQL
import io.qross.setting.{Global, Language}
import io.qross.time.Timer
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.internet._
import javax.mail.{Message, SendFailedException, Transport}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Email {
    
    //username/fullname -> (email, fullname)
    val RECEIVERS: Map[String, (String, String)] = {
        //get all recipients
        if (JDBC.hasQrossSystem) {
            val recipients = new mutable.HashMap[String, (String, String)]()
            val ds = DataSource.QROSS
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_users'")) {
                ds.executeDataTable("SELECT username, fullname, email FROM qross_users WHERE enabled='yes'")
                    .foreach(row => {
                        recipients += row.getString("username") -> (row.getString("email"), row.getString("fullname"))
                        recipients += row.getString("fullname") -> (row.getString("email"), row.getString("fullname"))
                    }).clear()
            }
            ds.close()

            recipients.toMap
        }
        else {
            Map[String, (String, String)]()
        }
    }

    def write(title: String): Email = {
        new Email(title)
    }

    implicit class DataHub$Email(val dh: DataHub) {

        def EMAIL: Email = {
            dh.pick[Email]("EMAIL") match {
                case Some(email) => email
                case None => throw new ExtensionNotFoundException("Must use writeEmail method to write an email first.")
            }
        }

        def writeEmail(title: String): DataHub = {
            dh.plug("EMAIL", new Email(title))
        }

        def setSmtpServer(host: String, port: String): DataHub = {
            EMAIL.setSmtpServer(host, port)
            dh
        }

        def setFrom(address: String, password: String, name: String = ""): DataHub = {
            EMAIL.setFrom(address, password, name)
            dh
        }

        def setFrom(fromName: String): DataHub = {
            EMAIL.setFrom(fromName)
            dh
        }

        def setFromPersonal(personal: String): DataHub = {
            EMAIL.setFromPersonal(personal)
            dh
        }

        def useDefaultEmailTemplate(): DataHub = {
            EMAIL.useDefaultTemplate()
            dh
        }

        def useEmailTemplate(template: String): DataHub = {
            EMAIL.useTemplate(template)
            dh
        }

        def withDefaultSignature(): DataHub = {
            EMAIL.withDefaultSignature()
            dh
        }

        def withSignature(resourceFile: String): DataHub = {
            EMAIL.withSignature(resourceFile)
            dh
        }

        def setEmailContent(content: String): DataHub = {
            EMAIL.setContent(content)
            dh
        }

        def placeEmailTitle(title: String): DataHub = {
            EMAIL.placeData("#{title}", title)
            dh
        }

        def placeEmailContent(content: String): DataHub = {
            EMAIL.placeData("#{content}", content)
            dh
        }

        def placeEmailDataTable(placeHolder: String = ""): DataHub = {
            if (placeHolder == "") {
                EMAIL.placeData("#{data}", dh.getData.toHtmlString())
            }
            else {
                EMAIL.placeData("#{" + placeHolder + "}", dh.getData.toHtmlString())
            }

            dh.preclear()
        }

        def placeEmailHolder(replacements: (String, String)*): DataHub = {
            for ((placeHolder, replacement) <- replacements) {
                EMAIL.placeData(placeHolder, replacement)
            }
            dh
        }

        def placeEmailContentWithRow(i: Int = 0): DataHub = {
            dh.getData.getRow(i) match {
                case Some(row) => EMAIL.placeData(row)
                case None =>
            }

            dh.preclear()
        }

        def placeEmailContentWithFirstRow(): DataHub = {
            dh.getData.firstRow match {
                case Some(row) => EMAIL.placeData(row)
                case _ =>
            }

            dh.preclear()
        }

        def to(recipients: String): DataHub = {
            EMAIL.to(recipients)

            dh
        }

        def cc(recipients: String): DataHub = {
            EMAIL.cc(recipients)

            dh
        }

        def bcc(recipients: String): DataHub = {
            EMAIL.bcc(recipients)

            dh
        }

        def attach(path: String): DataHub = {
            EMAIL.attach(path)

            dh
        }

        def send(): DataHub = {
            EMAIL.placeData("${title}", "")
            EMAIL.placeData("${content}", "")
            EMAIL.placeData("${data}", "")
            EMAIL.send()

            dh
        }
    }
}

class Email(private var title: String) {

    private val smtp = new java.util.Properties()

    smtp.setProperty("mail.transport.protocol", "smtp")
    smtp.setProperty("mail.smtp.host", Global.EMAIL_SMTP_HOST)
    smtp.setProperty("mail.smtp.auth", "true")
    smtp.setProperty("mail.smtp.socketFactory.port", Global.EMAIL_SMTP_PORT)
    smtp.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
    smtp.setProperty("mail.smtp.port", Global.EMAIL_SMTP_PORT)

    //系统默认值
    private val from = mutable.HashMap[String, String] (
        "account" -> Global.EMAIL_SENDER_ACCOUNT,
        "personal" -> Global.EMAIL_SENDER_PERSONAL,
        "password" -> Global.EMAIL_SENDER_PASSWORD)

    private var attachments: ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

    //email -> fullname
    private var toRecipients = new mutable.HashMap[String, String]()
    private var ccRecipients = new mutable.HashMap[String, String]()
    private var bccRecipients = new mutable.HashMap[String, String]()
    private var content: String = ""
    private var placement: String = "NOTHING"

    def setSmtpServer(host: String, port: String): Email = {
        smtp.setProperty("mail.smtp.host", host)
        smtp.setProperty("mail.smtp.socketFactory.port", port)
        smtp.setProperty("mail.smtp.port", port)
        this
    }

    def setSmtpHost(host: String): Email = {
        smtp.setProperty("mail.smtp.host", host)
        this
    }

    def setSmtpPort(port: String): Email = {
        smtp.setProperty("mail.smtp.socketFactory.port", port)
        smtp.setProperty("mail.smtp.port", port)
        this
    }

    def setFrom(address: String, password: String, personal: String = ""): Email = {
        from += "account" -> address
        from += "password" -> password
        from += "personal" -> personal
        this
    }

    def setFrom(personal: String): Email = {
        if (personal.contains("@")) {
            if (personal.contains("<") && personal.trim().endsWith(">")) {
                from += "personal" -> personal.takeBefore("<")
                from += "account" -> personal.takeAfter("<").takeBefore(">")
            }
            else {
                from += "account" -> personal
            }
        }
        else if (JDBC.hasQrossSystem) {
            val ds = DataSource.QROSS
            if (ds.executeExists("SELECT table_name FROM information_schema.TABLES WHERE table_schema=DATABASE() AND table_name='qross_email_senders'")) {
                val sender = ds.executeDataRow("SELECT smtp_host, smtp_port, sender_address, sender_password FROM qross_email_senders WHERE sender_personal=?", personal)
                if (sender.nonEmpty) {
                    val host = sender.getString("smtp_host")
                    val port = sender.getString("smtp_port")
                    if (host != "") {
                        smtp.setProperty("mail.smtp.host", host)
                    }
                    if (port != "") {
                        smtp.setProperty("mail.smtp.socketFactory.port", port)
                        smtp.setProperty("mail.smtp.port", port)
                    }

                    from += "account" -> sender.getString("sender_address")
                    from += "password" -> sender.getString("sender_password")
                    from += "personal" -> personal
                }
                else {
                    throw new EmailInvalidSenderException("Invalid sender personal: " + personal)
                }
            }
            ds.close()
        }
        this
    }

    //只设置发送人签名
    def setFromPersonal(personal: String): Email = {
        from += "personal" -> personal
        this
    }

    def setFromAddress(address: String): Email = {
        from += "account" -> address
        this
    }

    def setFromPassword(password: String): Email = {
        from += "password" -> password
        this
    }

    def useTemplate(template: String): Email = {

        //name, filePath - .htm/.html/.txt, content
        //io.qross.pql.PQL.runEmbeddedFile(args.head.asText).toString
        //PQL.runEmbedded(template).toString

        this.content = {
            if (template.trim().bracketsWith("<", ">") || (template.contains("<%") && template.contains("%>"))) {
                template
            }
            else if (template.contains(".") && """(?i)^htm|html|txt$""".r.test(template.takeAfterLast("."))) {
                if (template.contains("/") || template.contains("\\")) {
                    SourceFile.read(template)
                }
                else {
                    SourceFile.read(Global.QROSS_HOME + "templates/email/" +  template)
                }
            }
            else {
                DataSource.QROSS
                        .querySingleValue("SELECT template_content FROM qross_email_templates WHERE template_name=?", template)
                        .orElse(template, DataType.TEXT)
                        .asText
            }
        }
        this
    }

    def useDefaultTemplate(): Email = {
        useTemplate(Global.QROSS_HOME + "templates/email/default.html")
        this
    }

    def withDefaultSignature(): Email = {
        withSignature(Global.QROSS_HOME + "templates/email/signature.html")
    }

    def withSignature(signature: String): Email = {
        val code = {
            if (signature.bracketsWith("<", ">") || (signature.contains("<%") && signature.contains("%>"))) {
                signature
            }
            else if (signature.contains(".") && "(?i)^htm|html|txt$".r.test(signature.takeAfterLast("."))) {
                if (signature.contains("/") || signature.contains("\\")) {
                    SourceFile.read(signature)
                }
                else {
                    SourceFile.read(Global.QROSS_HOME + "templates/email/" +  signature)
                }
            }
            else {
                DataSource.QROSS
                        .querySingleValue("SELECT template_content FROM qross_email_templates WHERE template_name=?", signature)
                        .orElse(signature, DataType.TEXT)
                        .asText
            }
        }

        this.content = {
            if (this.content.contains("#{signature}")) {
                this.content.replace("#{signature}", code)
            }
            else if (this.content.contains("&{signature}")) {
                this.content.replace("&{signature}", code)
            }
            else if ("""(?i)</body>""".r.test(this.content)) {
                this.content.replaceAll("""</body>""", code + "</body>")
            }
            else {
                this.content + code
            }
        }

        this
    }

    def setTitle(title: String): Email = {
        this.title = title
        this
    }

    def setContent(content: String): Email = {
        this.content = if (this.content.contains("#{content}")) this.content.replace("#{content}", content) else content
        this
    }

    def place(replacement: String): Email = {
        this.placement = replacement
        this
    }

    def at(placeHolder: String): Email = {
        this.placeData(placeHolder, this.placement)
    }

    def placeData(placeHolder: String, replacement: String): Email = {
        this.content = this.content.replace(placeHolder, replacement)
        this
    }

    def placeData(queryString: String): Email = {
        placeData(queryString.$split().toRow)
    }

    def placeData(row: DataRow): Email = {
        this.content = this.content.replaceArguments(row)
        this
    }

    def placeData(map: Map[String, String]): Email = {
        this.content = this.content.replaceArguments(map)
        this
    }

    def attach(paths: String*): Email = {
        paths.foreach(path => {
            this.attachments += path.locate()
        })
        this
    }
    
    private def parseRecipients(recipients: String): mutable.HashMap[String, String] = {
        val map = new mutable.HashMap[String, String]()
        recipients.replace(",", ";").split(";").foreach(receiver => {
            var personal = ""
            var address = receiver.trim
            if (address.contains("<") && address.endsWith(">")) {
                personal = address.substring(0, address.indexOf("<"))
                address = address.substring(address.indexOf("<") + 1).dropRight(1)
            }
            else if (!address.contains("@")) {
                Email.RECEIVERS.get(address) match {
                    case Some(user) =>
                        address = user._1
                        personal = user._2
                    case None =>
                }
            }

            map.put(address, personal)
        })
        
        map
    }
    
    def to(recipients: String): Email = {
        if (recipients != null && recipients != "") {
            this.toRecipients ++= parseRecipients(recipients)
        }
        this
    }
    
    def cc(recipients: String): Email = {
        if (recipients != "") {
            this.ccRecipients ++= parseRecipients(recipients)
        }
        this
    }
    
    def bcc(recipients: String): Email = {
        if (recipients != "") {
            this.bccRecipients ++= parseRecipients(recipients)
        }
        this
    }

    def send(): DataRow = {
        if (Global.EMAIL_SENDER_ACCOUNT_AVAILABLED) {
            if (toRecipients.nonEmpty || ccRecipients.nonEmpty || bccRecipients.nonEmpty) {
                if (this.content != "") {
                    this.content = PQL.openEmbedded(this.content).run().toString
                }
                transfer()
            }
            else {
                new DataRow("message" -> "No recipients.")
            }
        }
        else {
            new DataRow("message" -> "Email sender account is not set.")
        }
    }

    def transfer(): DataRow = {

        val result = new DataRow("message" -> "Sent.")
        val logs = new java.util.ArrayList[String]()
        val sent = new java.util.ArrayList[String]()
        val invalid = new java.util.ArrayList[String]()

        val session = javax.mail.Session.getInstance(smtp)
        session.setDebug(false)
    
        val message = new MimeMessage(session)
    
        message.setFrom(new InternetAddress(from("account"), from("personal"), "UTF-8"))
        for((address, personal) <- toRecipients) {
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(address, personal, "UTF-8"))
        }
        for((address, personal) <- ccRecipients) {
            message.addRecipient(Message.RecipientType.CC, new InternetAddress(address, personal, "UTF-8"))
        }
        for((address, personal) <- bccRecipients) {
            message.addRecipient(Message.RecipientType.BCC, new InternetAddress(address, personal, "UTF-8"))
        }
        message.setSubject(title, "UTF-8")
        
        if (attachments.isEmpty) {
            message.setContent(content, "text/html;charset=UTF-8")
        }
        else {
            val mixedContent = new MimeMultipart()
            
            val contentBody = new MimeBodyPart()
            contentBody.setContent(content, "text/html;charset=UTF-8")
            mixedContent.addBodyPart(contentBody)
            for (file <- attachments) {
                val attachmentBody = new MimeBodyPart()
                val dh = new DataHandler(new FileDataSource(file))
                attachmentBody.setDataHandler(dh)
                attachmentBody.setFileName(MimeUtility.encodeText(dh.getName, "UTF-8", "B")) //B = base64
                mixedContent.addBodyPart(attachmentBody)
            }
            mixedContent.setSubType("mixed")
            
            message.setContent(mixedContent)
        }
        
        message.setSentDate(new java.util.Date())
        message.saveChanges()
   
        val transport: Transport = session.getTransport

        var retry = 0
        var connected = false
        while (!connected && retry < 10) {
            try {
                transport.connect(from("account"), from("password"))
                connected = true
            }
            catch {
                case e: Exception =>
                    e.printStackTrace()
                    retry += 1
                    Timer.sleep(1000)
            }
        }

        if (!connected) {
            logs.add("Connect mail server failed! Please retry or check your username and password.")
        }

        try {
            val allRecipients = message.getAllRecipients
            transport.sendMessage(message, allRecipients)
            allRecipients.foreach(address => sent.add(address.toString))
            logs.add("Sent to " + allRecipients.mkString(", ") + ".")
        }
        catch {
            case se: SendFailedException =>
                val invalidAddresses = se.getInvalidAddresses
                if (invalidAddresses != null && invalidAddresses.nonEmpty) {
                    invalidAddresses.foreach(address => invalid.add(address.toString))
                    logs.add("Invalid address(es) " + invalidAddresses.mkString(", ") + ".")
                }
                val sentAddresses = se.getValidSentAddresses
                if (sentAddresses != null && sentAddresses.nonEmpty) {
                    sentAddresses.foreach(address => sent.add(address.toString))
                    logs.add("Sent to valid address(es) " + sentAddresses.mkString(", ") + ".")
                }
                val validAddresses = se.getValidUnsentAddresses
                if (validAddresses != null && validAddresses.nonEmpty) {
                    transport.sendMessage(message, validAddresses)
                    validAddresses.foreach(address => sent.add(address.toString))
                    logs.add("Resend to valid address(es) " + validAddresses.mkString(", ") + ".")
                }
            case e: Exception =>
                logs.add(e.getMessage)
                result.set("message", e.getMessage)
                e.printStackTrace()
        }
        transport.close()
        
        title = ""
        content = ""

        toRecipients.clear()
        ccRecipients.clear()
        bccRecipients.clear()
        attachments.clear()

        if (logs.isEmpty) {
            result.set("message", "Sent but maybe something wrong.")
        }

        result.set("logs", logs, DataType.ARRAY)
        result.set("sent", sent, DataType.ARRAY)
        result.set("invalid", invalid, DataType.ARRAY)

        result
    }
}

/*
    public static MimeMessage createMyEmail(Session session, String sendMail, String receiveMail) throws Exception {
        MimeMessage message = new MimeMessage(session);
        //1. 发件人信息
        message.setFrom(new InternetAddress(sendMail, "163User", "UTF-8"));
        //2. 收件人信息
        message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(receiveMail, "weShareUser", "UTF-8"));
    /**  增加收件人（可选）
        message.addRecipient(MimeMessage.RecipientType.TO, new InternetAddress("dd@receive.com", "USER_DD", "UTF-8"));
         // 添加抄送
        message.setRecipient(MimeMessage.RecipientType.CC, new InternetAddress(copyReceiveMailAccount, "xijie", "UTF-8"));
        // 密送（可选）
        message.setRecipient(MimeMessage.RecipientType.BCC, new InternetAddress("ff@receive.com", "USER_FF", "UTF-8"));*/
        //3. 邮件主题
        message.setSubject("来自wangyue邮箱的邮件", "UTF-8");

        // 4.邮件正文  step1: 创建图片节点，读取本地的图片文件
        MimeBodyPart image = new MimeBodyPart();
        DataHandler handler = new DataHandler(new FileDataSource("C:\\Users\\12108\\Downloads\\image.jpg"));
        // 将图片数据添加到“节点”
        image.setDataHandler(handler);
        // 为“节点”设置一个唯一编号（在文本“节点”将引用该ID）
        image.setContentID("image_mountain");

        // step2: 创建图片的文本节点
        MimeBodyPart text = new MimeBodyPart();
        text.setContent("这是一张图片<br/><img src='cid:image_mountain'/>", "text/html;charset=UTF-8");

        // step3: （文本+图片）设置 文本 和 图片 “节点”的关系（将 文本 和 图片 “节点”合成一个混合“节点”）
        MimeMultipart multipart = new MimeMultipart();
        multipart.addBodyPart(image);
        multipart.addBodyPart(text);
        // 关联关系
        multipart.setSubType("related");

        // step4: 将文本+图片的multipart转换成MimeBodyPart
        MimeBodyPart textImage = new MimeBodyPart();
        textImage.setContent(multipart);

        // step5: 创建附件“节点”
        MimeBodyPart attachment = new MimeBodyPart();
        DataHandler dh = new DataHandler(new FileDataSource("C:\\Users\\12108\\Desktop\\blacklist.txt"));
        attachment.setDataHandler(dh);
        // 设置附件的文件名（需要编码）
        attachment.setFileName(MimeUtility.encodeText(dh.getName(),"UTF-8","B"));

        // html文件
        MimeBodyPart textTwo = new MimeBodyPart();
        String readfile = readFile("C:\\Users\\12108\\Downloads\\slow_mysql(1).html");
        textTwo.setContent(readfile, "text/html;charset=UTF-8");

        // step6: 设置（文本+图片）和 附件 的关系（合成一个大的混合“节点” / Multipart ）
        MimeMultipart textImageAttachment = new MimeMultipart();
        textImageAttachment.addBodyPart(textImage);
        textImageAttachment.addBodyPart(attachment);
        textImageAttachment.addBodyPart(textTwo);
        textImageAttachment.setSubType("mixed");

        // step7: 设置整个邮件的关系（将最终的混合“节点”作为邮件的内容添加到邮件对象）
        message.setContent(textImageAttachment);

        //5. 设置发送时间
        message.setSentDate(new Date());

        //6. 保存设置
        message.saveChanges();
        // 返回包含了所有信息的message
        return message;
    }
  */