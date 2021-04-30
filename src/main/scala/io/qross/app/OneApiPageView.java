package io.qross.app;

import io.qross.time.DateTime;

import java.util.Map;

public class OneApiPageView {

    public int userId;
    public int apiId;
    public Map<String, Object> parameters;
    public long elapsed;
    public String requestTime;

    public OneApiPageView(int userId, int apiId, Map<String, Object> parameters, long elapsed) {
        this.userId = userId;
        this.apiId = apiId;
        this.parameters = parameters;
        this.elapsed = elapsed;
        this.requestTime = DateTime.now().getString("yyyy-MM-dd HH:mm:ss");
    }
}
