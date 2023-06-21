package com.atguigu.day11.util;

public class UrlViewCount {
    public String url;
    public Long windowEnd;
    public Long count;

    public UrlViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
