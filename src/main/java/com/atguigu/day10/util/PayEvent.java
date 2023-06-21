package com.atguigu.day10.util;
/**
 * 第三方机构的支付事件
 * weixin zhifubao
 */
public class PayEvent {
    public String orderId;
    public String eventType;
    public Long eventTime;

    public PayEvent(String orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public PayEvent() {
    }

    @Override
    public String toString() {
        return "PayEvent{" +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
