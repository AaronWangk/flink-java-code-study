package com.atguigu.day10;

import com.atguigu.day10.util.OrderEvent;
import com.atguigu.day10.util.PayEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RealTimeCheckBill {

    private static OutputTag<String> unmatchedOrders = new OutputTag<String>("order"){};
    private static OutputTag<String> unmatchedPays   = new OutputTag<String>("pay"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> orderStream = env.fromElements(
                new OrderEvent("order_1", "pay", 2000L),
                new OrderEvent("order_2", "pay", 3000L),
                new OrderEvent("order_3", "pay", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })
                );


        DataStream<PayEvent> payStream = env.fromElements(
                new PayEvent("order_1","zhifubao",5000L),
                new PayEvent("order_4","zhifubao",6000L),
                new PayEvent("order_5","zhifubao",4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<PayEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PayEvent>() {
                            @Override
                            public long extractTimestamp(PayEvent element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })

                );

        SingleOutputStreamOperator<String> process =
                orderStream
                .connect(payStream)
                .keyBy(order -> order.orderId, pay -> pay.orderId)
                .process(new MatchFunction());

        process.print();//打印正常的核对上的流
        process.getSideOutput(unmatchedOrders).print();//没有连接上第三方订单信息的流输出

        process.getSideOutput(unmatchedPays).print();//没有连接上订单信息的流输出


        env.execute();


    }


    public static class MatchFunction extends CoProcessFunction<OrderEvent, PayEvent, String> {

        private ValueState<OrderEvent> orderState;
        private ValueState<PayEvent> payState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在 open() 方法中进行初始化或设置操作
            // 可以访问运行时上下文和配置信息
            // 执行一次性的准备工作
            super.open(parameters);
            orderState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class)
            );
            payState = getRuntimeContext().getState(
                    new ValueStateDescriptor<PayEvent>("pay", PayEvent.class)
            );
        }


        @Override
        public void processElement1(OrderEvent in, Context ctx, Collector<String> out) throws Exception {
            PayEvent orderStates = payState.value();
            if (orderStates != null){
                //第三方支付已经connect上了
                payState.clear();
                out.collect("order id " + in.orderId + " matched success");

            }else{
                //第三方支付没connect上
                //设置一个定时器等待第三方流5秒钟
                orderState.update(in);
                ctx.timerService().registerEventTimeTimer( 5 * 1000L);
            }
        }

        @Override
        public void processElement2(PayEvent in, Context ctx, Collector<String> out) throws Exception {
            OrderEvent orderEvent = orderState.value();

            if (orderEvent != null ){
                //订单信息已经connect上了
                orderState.clear();
                out.collect("order id " + in.orderId + " matched success");
            }else{
                //订单信息没有connect上
                //设置一个定时器等待订单流5秒钟
                payState.update(in);
                ctx.timerService().registerEventTimeTimer( 5 * 1000L);
            }
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (orderState.value() != null ){
                //依然没有获取到第三方支付信息
                //推送至未匹配订单流
                ctx.output(unmatchedOrders, "order id: " + orderState.value().orderId + " not match");
                orderState.clear();
            }

            if (payState.value() != null) {
                //依然没有获取到订单信息
                //推送至未匹配订单支付流
                ctx.output(unmatchedPays, "order id: " + payState.value().orderId + " not match");
                payState.clear();
            }

        }

    }


}
