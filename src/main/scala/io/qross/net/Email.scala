package io.qross.net

import io.qross.core.DataRow
import io.qross.fs.ResourceFile
import io.qross.fs.FilePath._
import io.qross.jdbc.{DataSource, JDBC}
import io.qross.setting.Global
import io.qross.time.Timer
import io.qross.ext.Output
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.internet._
import javax.mail.{Message, SendFailedException, Session, Transport}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Email {
    
    //username/fullname -> (email, fullname)
    val AllReceivers = new mutable.HashMap[String, (String, String)]()

    //get all receivers
    if (JDBC.hasQrossSystem) {
        DataSource.queryDataTable("SELECT username, fullname, email FROM qross_users WHERE enabled='yes'")
                .foreach(row => {
                    AllReceivers += row.getString("username") -> (row.getString("email"), row.getString("fullname"))
                    AllReceivers += row.getString("fullname") -> (row.getString("email"), row.getString("fullname"))
                }).clear()
    }

    def write(title: String): Email = {
        new Email(title)
    }
}

class Email(private var title: String) {

    private var attachments: ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

    //email -> fullname
    private var toReceivers = new mutable.HashMap[String, String]()
    private var ccReceivers = new mutable.HashMap[String, String]()
    private var bccReceivers = new mutable.HashMap[String, String]()
    private var content: String = if (Global.EMAIL_DEFAULT_TEMPLATE != "") ResourceFile.open(Global.EMAIL_DEFAULT_TEMPLATE).output else ""

    def fromTemplate(path: String): Email = {
        this.content = ResourceFile.open(path).output
        this
    }

    def withDefaultSignature(): Email = {
        this.content = this.content.replace("${signature}", ResourceFile.open(Global.EMAIL_DEFAULT_SIGNATURE).output)
        this
    }

    def withSignature(path: String): Email = {
        this.content = this.content.replace("${signature}", ResourceFile.open(path).output)
        this
    }
    
    def setTitle(title: String): Email = {
        this.title = title
        this
    }
    
    def setContent(content: String): Email = {
        this.content = if (this.content.contains("${content}")) this.content.replace("${content}", content) else content
        this
    }

    def placeContent(placeHolder: String, replacement: String): Email = {
        this.content = this.content.replace(placeHolder, replacement)
        this
    }

    def placeContentWidthDataRow(row: DataRow): Email = {
        row.foreach((key, value) => {
            this.content = this.content.replace("${" + key + "}", if (value != null) value.toString else "")
        })
        this
    }
    
    //def readContentFromUrl(url: String): Email = {
    //    this
    //}
    
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
                Email.AllReceivers.get(address) match {
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
                    Timer.sleep(1)
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