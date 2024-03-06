package cn.qross.security;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.springframework.util.DigestUtils;

public class MD5 {

    public static String encrypt(String str) {
        StringBuilder result = new StringBuilder();
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(str.getBytes("UTF8"));
            for (byte s : m.digest()) {
                result.append(Integer.toHexString((0x000000FF & s) | 0xFFFFFF00).substring(6));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    public static String encrypt(String str, String salt) {
        return DigestUtils.md5DigestAsHex((str + salt).getBytes());
    }

    //qc3ebXEyijPIPDgISujZqw==
//    public static String DEFAULT = encode("1234567");
//
//    public static String encode(String value) {
//        String str = "HELLO ";
//        try {
//            MessageDigest md5 = MessageDigest.getInstance("MD5");
//            str = Base64Encode(md5.digest((value + "QROSS").getBytes("utf-8")));
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//        return str;
//    }
//
}
