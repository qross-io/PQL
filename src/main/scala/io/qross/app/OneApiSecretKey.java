package io.qross.app;

import io.qross.time.DateTime;

public class OneApiSecretKey {
    public String token;
    public DateTime createTime;

    public OneApiSecretKey(String token) {
        this.token = token;
        createTime = DateTime.now();
    }

    public boolean expired() {
        return createTime.earlier(DateTime.now()) / 1000 > Setting.OneApiSecretKeyTTL;
    }

    public String getToken() {
        return token;
    }

    public long getTTL() {
        return Setting.OneApiSecretKeyTTL - createTime.earlier(DateTime.now()) / 1000;
    }
}
