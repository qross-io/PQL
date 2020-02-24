package io.qross.test;

import io.qross.core.DataRow;
import io.qross.core.DataTable;
import io.qross.ext.Console;
import io.qross.jdbc.DataAccess;
import io.qross.time.DateTime;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeUtility;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


class Collector {
    String name;
    String workplace;
    String title;
    String district;
    String isCollector;

    public Collector(String district, String workplace, String title, String name, String isCollector) {
        this.district = district;
        this.workplace = workplace;
        this.title = title;
        this.name = name;
        this.isCollector = isCollector;
    }
}

public class Pop3 {


    // 邮箱附件保存路径
    private static String FILE_SAVE_PATH = "";
    // 读取的发件人
    private static String fromEmail = "";

    private static String hostServer = "pop.exmail.qq.com";
    private static String protocol = "pop3";
    private static String port = "995";
//    private static String username = "wuzheng@zichan360.com";
//    private static String password = "Zichan20180709";
    private static String username = "commit.reply@zichan360.com";
    private static String password = "w9^$!9cXeeLY1";

    private static List<String> attachments = new ArrayList<>();
    private static List<String> contents = new ArrayList<>();

    private static Map<String, Collector> collectors = new HashMap<>();

    public static void main(String[] args) {

        DataAccess ds = new DataAccess("mysql.rds");
        //ds.executeNonQuery("truncate table commit_reply_result");
        DataTable table = ds.executeDataTable("SELECT * FROM commit_reply_staff");
        for (DataRow row : table.getRowList()) {
            collectors.put(row.getString("email"), new Collector(row.getString("district"), row.getString("workplace"), row.getString("title"), row.getString("staff"), row.getString("is_collector")));
        }
        table.clear();

        try {
            Session session;
            if ("pop3".equalsIgnoreCase(protocol)) {
                session = Session.getInstance(getPOP3());
//                session = Session.getDefaultInstance(getPOP3());
            } else {
//                session = Session.getDefaultInstance(getIMAP());
                session = Session.getInstance(getIMAP());
            }
            session.setDebug(false);

            Store store = session.getStore(protocol);
            store.connect(hostServer, username, password);

            Folder defaultFolder = store.getDefaultFolder();
            if (defaultFolder == null) {
                Console.writeException("Server is not aviliable. hostServer:{}", hostServer);
                return;
            }

            Folder folder = defaultFolder.getFolder("INBOX");
            folder.open(Folder.READ_WRITE);

            Message[] messages = folder.getMessages();
            DataTable mails = new DataTable();

            for (Message message : messages) {

                String subject = message.getSubject();
                if (subject == null) {
                    subject = "";
                }
                if (subject.length() > 200) {
                    subject = subject.substring(0, 200);
                }
                String from = pickAddress(message.getFrom()[0].toString());
                String sent = new DateTime(message.getSentDate(), "").toString();
                List<String> addresses = new ArrayList<>();

                Address[] recipients = message.getAllRecipients();
                if (recipients != null) {
                    for (Address recipient : message.getAllRecipients()) {
                        String address = pickAddress(recipient.toString());
                        if (!address.equals("commit.reply@zichan360.com")) {
                            addresses.add(address);
                        }
                    }
                }

                analysisContent(message.getContent());
                analysisAttachments(message.getContent());

                String content = String.join(";", contents);
                String standard = "否";
                if (content.contains("确认收到邮件并遵守承诺书内容") && content.contains("处于抗击疫情的特殊时期") && content.contains("本人答复邮件的行为与在承诺书签章具有同一法律效力")) {
                    standard = "是";
                }
                else if (subject.contains("确认收到邮件并遵守承诺书内容") && subject.contains("处于抗击疫情的特殊时期") && subject.contains("本人答复邮件的行为与在承诺书签章具有同一法律效力")) {
                    standard = "是";
                }
                if (content.length() > 65535) {
                    content = content.substring(0, 65000);
                }

                String isCollector = "否";
                String name = "";
                String district = "";
                String workplace = "";
                String title = "";
                if (collectors.containsKey(from)) {
                    isCollector = collectors.get(from).isCollector;
                    name = collectors.get(from).name;
                    district = collectors.get(from).district;
                    workplace = collectors.get(from).workplace;
                    title = collectors.get(from).title;
                }

                DataRow row = new DataRow();
                row.set("sender", from);
                row.set("sent_time", sent);
                row.set("is_collector", isCollector);
                row.set("title", title);
                row.set("staff", name);
                row.set("district", district);
                row.set("workplace", workplace);
                row.set("subject", subject);
                row.set("cc", String.join(";", addresses));
                row.set("content", content);
                row.set("is_standard", standard);
                row.set("attachments", String.join(";", attachments));
                mails.addRow(row);

                contents.clear();
                attachments.clear();

                Console.writeLine(subject);
            }

            ds.tableUpdate("INSERT INTO commit_reply_result (sender, sent_time, is_collector, title, staff, district, workplace, subject, cc, content, is_standard, attachments) \n" +
                    "    SELECT '#sender', '#sent_time', '#is_collector', '#title', '#staff', '#district', '#workplace', '#subject', '#cc', '#content', '#is_standard', '#attachments'\n" +
                    "        FROM dual WHERE NOT EXISTS (SELECT id FROM commit_reply_result WHERE sender='#sender' AND sent_time='#sent_time');", mails);

            ds.close();

//            SearchTerm comparisonTermGe = new SentDateTerm(ComparisonTerm.GE, start.toDate());
//            SearchTerm comparisonTermLe = new SentDateTerm(ComparisonTerm.LE, end.toDate());
//            FromStringTerm fromStringTerm = new FromStringTerm(fromEmail);
//            SearchTerm andTerm = new AndTerm(new SearchTerm[]{comparisonTermGe, comparisonTermLe, fromStringTerm});
//            //Console.writeMessage("SearchTerm start: {}, end: {}, fromEmail: {}", start.toString(), end.toString(), fromEmail);
//
//            Message[] messages = folder.search(andTerm); //根据设置好的条件获取message
//            Console.writeMessage("search邮件: " + messages.length + "封, SearchTerm:" + andTerm.getClass());
//            // FetchProfile fProfile = new FetchProfile(); // 选择邮件的下载模式,
//            // fProfile.add(FetchProfile.Item.ENVELOPE); // 根据网速选择不同的模式
//            // folder.fetch(messages, fProfile);// 选择性的下载邮件
//            // 5. 循环处理每个邮件并实现邮件转为新闻的功能
//            for (int i = 0; i < messages.length; i++) {
//                // 单个邮件
//                Console.writeMessage("---第" + i + "邮件开始------------");
//                mailReceiver(messages[i]);
//                Console.writeMessage("---第" + i + "邮件结束------------");
//                // 邮件读取备份保存，用来校验
////            messages[i].writeTo(new FileOutputStream(FILE_SAVE_PATH + "pop3Mail_" + messages[i].getMessageNumber() + ".eml"));
//            }

            // 7. 关闭 Folder 会真正删除邮件, false 不删除
            folder.close(false);
            // 8. 关闭 store, 断开网络连接
            store.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String pickAddress(String recipient) {
        if (recipient.contains("<")) {
            recipient = recipient.substring(recipient.indexOf("<") + 1);
        }

        if (recipient.contains(">")) {
            recipient = recipient.substring(0, recipient.indexOf(">"));
        }

        return recipient.toLowerCase();
    }

    private static void analysisContent(Object content) {
        try {
            if (content instanceof Multipart) {
                Multipart part = (Multipart) content;
                for (int j = 0; j < part.getCount(); j++) {
                    analysisContent(part.getBodyPart(j));
                }
            } else if (content instanceof Part) {
                Part part = (Part) content;
                analysisContent(part.getContent());
            } else {
                contents.add(content.toString());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void analysisAttachments(Object content) {
        try {
            if (content instanceof Multipart) {
                Multipart part = (Multipart) content;
                for (int j = 0; j < part.getCount(); j++) {
                    String file = part.getBodyPart(j).getFileName();
                    if (file != null) {
                        file = MimeUtility.decodeText(file);
                        attachments.add(file);
                    }
                }
            } else if (content instanceof Part) {
                Part part = (Part) content;
                String file = part.getFileName();
                if (file != null) {
                    file = MimeUtility.decodeText(file);
                    attachments.add(part.getFileName());
                }
            } else {
                contents.add(content.toString());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取POP3收信配置 995
     *
     * @return
     */
    private static Properties getPOP3() {
        Properties props = new Properties();
        props.setProperty("mail.transport.protocol", protocol);
        props.setProperty("mail.pop3.host", hostServer); // 按需要更改
        props.setProperty("mail.pop3.port", port);
        // SSL安全连接参数
        props.setProperty("mail.pop3.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.setProperty("mail.pop3.socketFactory.fallback", "false");
        props.setProperty("mail.pop3.socketFactory.port", port);
        // 解决DecodingException: BASE64Decoder: but only got 0 before padding character (=)
        props.setProperty("mail.mime.base64.ignoreerrors", "true");
        return props;
    }

    /**
     * 获取IMAP收信配置 993
     *
     * @return
     */
    private static Properties getIMAP() {
        Properties props = new Properties();
        props.setProperty("mail.transport.protocol", protocol);
        props.setProperty("mail.imap.host", hostServer); // 按需要更改
        props.setProperty("mail.imap.port", port);
        // SSL安全连接参数
        props.setProperty("mail.imap.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.setProperty("mail.imap.socketFactory.fallback", "false");
        props.setProperty("mail.imap.socketFactory.port", port);
        props.setProperty("mail.mime.base64.ignoreerrors", "true");
        return props;
    }



    /**
     * 解析邮件
     *
     * @param msg 邮件对象
     * @throws Exception
     */
    private void mailReceiver(Message msg) {
        try {
            // 发件人信息
            Address[] froms = msg.getFrom();
            String mailSubject = transferChinese(msg.getSubject());
            if (froms != null) {
                InternetAddress addr = (InternetAddress) froms[0];
                Console.writeMessage("发件人地址:" + addr.getAddress() + ", 发件人显示名:" + transferChinese(addr.getPersonal()));
            } else {
                Console.writeException("msg.getFrom() is null... subject:" + mailSubject);
            }
            Date sentDate = msg.getSentDate();
            Console.writeMessage("邮件主题: {}, sentDate: {}", mailSubject,
                    sentDate == null ? null : sentDate);

            // getContent() 是获取包裹内容, Part相当于外包装
            Object content = msg.getContent();
            if (content instanceof Multipart) {
                Multipart multipart = (Multipart) content;
                reMultipart(multipart);
            } else if (content instanceof Part) {
                Part part = (Part) content;
                rePart(part);
            } else {
                String contentType = msg.getContentType();
                if (contentType != null && contentType.startsWith("text/html")) {
                    Console.writeWarning("---类型:" + contentType);
                } else {
                    Console.writeWarning("---类型:" + contentType);
                    Console.writeWarning("---内容:" + msg.getContent());
                }
            }
        } catch (Exception e) {
            Console.writeException(e.getMessage(), e);
        }
    }

    /**
     * 把邮件主题转换为中文.
     *
     * @param strText the str text
     * @return the string
     */
    public String transferChinese(String strText) {
        try {
            if (strText == null || strText.isEmpty()) {
                return null;
            }
            strText = MimeUtility.encodeText(new String(strText.getBytes(),
                    "UTF-8"), "UTF-8", "B");
            strText = MimeUtility.decodeText(strText);
        } catch (Exception e) {
            Console.writeException(e.getMessage(), e);
        }
        return strText;
    }


    /**
     * @param part 解析内容
     * @throws Exception
     */
    private void rePart(Part part) {
        String tempFilePath = null;
        try {
            // 附件
            if (part.getDisposition() != null) {
                // 邮件附件
                String strFileName = MimeUtility.decodeText(part.getFileName()); //MimeUtility.decodeText解决附件名乱码问题
                Console.writeMessage("发现附件: {}, 内容类型: {} ", strFileName, MimeUtility.decodeText(part.getContentType()));
                // 读取附件字节并存储到文件中. xls/xlsx
                String fileType = strFileName.substring(strFileName.lastIndexOf(".") + 1);
                if ((fileType.equals("xlsx") || fileType.equals("xls")) &&
                        (strFileName.contains("A") || strFileName.contains("B"))) {
                    InputStream in = part.getInputStream();// 打开附件的输入流
                    tempFilePath = FILE_SAVE_PATH + strFileName;
                    FileOutputStream out = new FileOutputStream(tempFilePath);
                    int data;
                    while ((data = in.read()) != -1) {
                        out.write(data);
                    }
                    in.close();
                    out.close();
                } else {
                    Console.writeMessage("not what we need file, discard it: {}", strFileName);
                }
            } else {
                // 邮件内容
                if (part.getContentType().startsWith("text/plain") || part.getContentType().startsWith("Text/Plain")) {
                    Console.writeMessage("Content文本内容：" + part.getContent());
                } else if (part.getContentType().startsWith("text/html")) {
//                    Console.writeMessage("HTML内容：" + part.getContent());
                    Console.writeDebugging("HTML内容，，不记录日志展示。。");
                } else {
                    Console.writeDebugging("!其它ContentType:" + part.getContentType() + " ?内容：" + part.getContent());
                }
            }
        } catch (MessagingException e) {
            Console.writeException(e.getMessage(), e);
        } catch (IOException e) {
            Console.writeException(e.getMessage(), e);
        } finally {
            // 单个处理黑名单文件，放入线程池处理
            String finalTempFilePath = tempFilePath;
            dealBlacklist(finalTempFilePath);

        }
    }


    /**
     * @param multipart // 接卸包裹（含所有邮件内容(包裹+正文+附件)）
     * @throws Exception
     */
    private void reMultipart(Multipart multipart) throws Exception {

        Console.writeDebugging("Multipart邮件共有" + multipart.getCount() + "部分组成");
        // 依次处理各个部分
        for (int j = 0, n = multipart.getCount(); j < n; j++) {
            Part part = multipart.getBodyPart(j);
            // 解包, 取出 MultiPart的各个部分, 每部分可能是邮件内容, 也可能是另一个小包裹(MultipPart)
            if (part.getContent() instanceof Multipart) {
                Console.writeDebugging("部分" + j + "的ContentType: " + part.getContentType() + ", to reMultipart() ");
                Multipart p = (Multipart) part.getContent();// 转成小包裹
                //递归迭代
                reMultipart(p);
            } else {
                Console.writeDebugging("部分" + j + "的ContentType: " + part.getContentType() + ", to rePart() ");
                rePart(part);
            }
        }
    }

    /**
     * 单个处理下载好的文件
     *
     * @param tempFilePath
     */
    private void dealBlacklist(String tempFilePath) {
        if (tempFilePath == null || tempFilePath == "") {
            return;
        }
        Console.writeMessage("to deal with blacklist Excel file: {}", tempFilePath);
        //blacklistService.dealBlacklist(tempFilePath);

    }

}