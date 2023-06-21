package com.atguigu.day11.util;

public class ApacheLog {
    public String ipAddr;
    public String userId;
    public Long eventTime;
    public String method;
    public String url;


    public ApacheLog(String ipAddr, String userId, Long eventTime, String method, String url) {
        this.ipAddr = ipAddr;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLog{" +
                "ipAddr='" + ipAddr + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
