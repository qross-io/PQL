package cn.qross.security;

public class Base64 {

    public static String encode(String text) {
        return new String(org.apache.commons.codec.binary.Base64.encodeBase64(text.getBytes()));
    }

    public static String encode(String text, String salt) {
        return new String(org.apache.commons.codec.binary.Base64.encodeBase64((salt + text).getBytes()));
    }

    public static String decode(String crypto) {
        return new String(org.apache.commons.codec.binary.Base64.decodeBase64(crypto.getBytes()));
    }

    public static String decode(String crypto, String salt) {
        String code = new String(org.apache.commons.codec.binary.Base64.decodeBase64(crypto.getBytes()));
        if (code.startsWith(salt)) {
            return code.substring(salt.length());
        }
        else {
            return code;
        }
    }
}
