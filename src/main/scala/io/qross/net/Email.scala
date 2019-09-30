package io.qross.net

import io.qross.core._
import io.qross.ext.Output
import io.qross.ext.TypeExt._
import io.qross.fs.FilePath._
import io.qross.fs.SourceFile
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.pql.PQL
import io.qross.pql.Solver._
import io.qross.setting.Global
import io.qross.time.Timer
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.internet._
import javax.mail.{Message, SendFailedException, Session, Transport}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Email {
    
    //username/fullname -> (email, fullname)
    val RECEIVERS: Map[String, (String, String)] = {
        //get all receivers
        if (JDBC.hasQrossSystem) {
            val receivers = new mutable.HashMap[String, (String, String)]()
            DataSource.QROSS.queryDataTable("SELECT username, fullname, email FROM qross_users WHERE enabled='yes'")
                    .foreach(row => {
                        receivers += row.getString("username") -> (row.getString("email"), row.getString("fullname"))
                        receivers += row.getString("fullname") -> (row.getString("email"), row.getString("fullname"))
                    }).clear()

            receivers.toMap
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
            if (dh.slots("EMAIL")) {
                dh.pick("EMAIL").asInstanceOf[Email]
            }
            else {
                throw new SlotObjectNotFoundException("Must use writeEmail method to write an email first.")
            }
        }

        def writeEmail(title: String): DataHub = {
            dh.plug("EMAIL", new Email(title))
        }

        def fromTemplate(template: String): DataHub = {
            EMAIL.fromTemplate(template)
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
                EMAIL.placeData("#{data}", dh.getData.toHtmlString)
            }
            else {
                EMAIL.placeData("#{" + placeHolder + "}", dh.getData.toHtmlString)
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
                case Some(row) => EMAIL.placeDataWithRow(row)
                case None =>
            }

            dh.preclear()
        }

        def placeEmailContentWithFirstRow(): DataHub = {
            dh.getData.firstRow match {
                case Some(row) => EMAIL.placeDataWithRow(row)
                case _ =>
            }

            dh.preclear()
        }

        def to(receivers: String): DataHub = {
            EMAIL.to(receivers)

            dh
        }

        def cc(receivers: String): DataHub = {
            EMAIL.cc(receivers)

            dh
        }

        def bcc(receivers: String): DataHub = {
            EMAIL.bcc(receivers)

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

    private var attachments: ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

    //email -> fullname
    private var toReceivers = new mutable.HashMap[String, String]()
    private var ccReceivers = new mutable.HashMap[String, String]()
    private var bccReceivers = new mutable.HashMap[String, String]()
    private var content: String = ""

    def fromTemplate(template: String): Email = {

        //name, filePath - .htm/.html/.txt, content
        //io.qross.pql.PQL.runEmbeddedFile(args.head.asText).toString
        //PQL.runEmbedded(template).toString

        this.content = {
            if (template.trim().bracketsWith("<", ">") || (template.contains("<%") && template.contains("%>"))) {
                template
            }
            else if (template.contains(".") && "(?i)^htm|html|txt$".r.test(template.takeAfter("."))) {
                SourceFile.read(template)
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

    def fromDefaultTemplate(): Email = {
        fromTemplate(Global.EMAIL_DEFAULT_TEMPLATE)
        this
    }

    def withDefaultSignature(): Email = {
        withSignature(Global.EMAIL_DEFAULT_SIGNATURE)
    }

    def withSignature(signature: String): Email = {
        val code = {
            if (signature.bracketsWith("<", ">") || (signature.contains("<%") && signature.contains("%>"))) {
                signature
            }
            else if (signature.contains(".") && "(?i)^htm|html|txt$".r.test(signature.takeAfter("."))) {
                SourceFile.read(signature)
            }
            else {
                DataSource.QROSS
                        .querySingleValue("SELECT signature_content FROM qross_email_signatures WHERE signature_name=?", signature)
                        .orElse(signature, DataType.TEXT)
                        .asText
            }
        }

        this.content = {
            if (this.content.contains("#{signature}")) {
                this.content.replace("#{signature}", code)
            }
            else if ("""(?i)</body>""".r.test(this.content)) {
                this.content.replaceAll("""</body>""", code)
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

    def placeData(placeHolder: String, replacement: String): Email = {
        this.content = this.content.replace(placeHolder, replacement)
        this
    }

    def placeData(queryString: String): Email = {
        queryString.$split()
            .foreach(k => {
                this.content = this.content.replace(k._1, k._1)
            })
        this
    }

    def placeDataWithRow(row: DataRow): Email = {
        this.content = this.content.replaceArguments(row)
        this
    }

    def attach(paths: String*): Email = {
        paths.foreach(path => {
            this.attachments += path.locate()
        })
        this
    }
    
    private def parseReceivers(receivers: String): mutable.HashMap[String, String] = {
        val map = new mutable.HashMap[String, String]()
        receivers.replace(",", ";").split(";").foreach(receiver => {
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
    
    def to(receivers: String): Email = {
        if (receivers != "") {
            this.toReceivers ++= parseReceivers(receivers)
        }
        this
    }
    
    def cc(receivers: String): Email = {
        if (receivers != "") {
            this.ccReceivers ++= parseReceivers(receivers)
        }
        this
    }
    
    def bcc(receivers: String): Email = {
        if (receivers != "") {
            this.bccReceivers ++= parseReceivers(receivers)
        }
        this
    }

    def send(): Unit = {
        if (toReceivers.nonEmpty || ccReceivers.nonEmpty || bccReceivers.nonEmpty) {
            this.content = PQL.runEmbedded(this.content).toString
            transfer()
        }
    }
    
    def transfer(): Unit = {
        val props = new java.util.Properties()
        props.setProperty("mail.transport.protocol", "smtp")
        props.setProperty("mail.smtp.host", Global.EMAIL_SMTP_HOST)
        props.setProperty("mail.smtp.auth", "true")
        if (Global.EMAIL_SSL_AUTH_ENABLED) {
            props.setProperty("mail.smtp.socketFactory.port", Global.EMAIL_SMTP_PORT)
            props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
        }
        else {
            props.setProperty("mail.smtp.port", Global.EMAIL_SMTP_PORT)
        }
    
        val session = Session.getInstance(props)
        session.setDebug(false)
    
        val message = new MimeMessage(session)
    
        message.setFrom(new InternetAddress(Global.EMAIL_SENDER_ACCOUNT, Global.EMAIL_SENDER_PERSONAL, "UTF-8"))
        for((address, personal) <- toReceivers) {
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(address, personal, "UTF-8"))
        }
        for((address, personal) <- ccReceivers) {
            message.addRecipient(Message.RecipientType.CC, new InternetAddress(address, personal, "UTF-8"))
        }
        for((address, personal) <- bccReceivers) {
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
                transport.connect(Global.EMAIL_SENDER_ACCOUNT, Global.EMAIL_SENDER_PASSWORD)
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
            throw new Exception("Connect mail server failed! Please retry or check your username and password.");
        }

        try {
            transport.sendMessage(message, message.getAllRecipients)
        }
        catch {
            case se: SendFailedException =>
                    se.getInvalidAddresses
                        .foreach(address => {
                        Output.writeException(s"Invalid email address $address")
                    })
                    transport.sendMessage(message, se.getValidUnsentAddresses)
            case e: Exception => e.printStackTrace()
        }
        transport.close()
        
        title = ""
        content = ""

        toReceivers.clear()
        ccReceivers.clear()
        bccReceivers.clear()
        attachments.clear()
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