package com.atguigu.day9;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class UserBehaviorAnalysisSelf {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //开始连接数据源
        DataStream<UserBehavior> stream = env.readTextFile("F:/flink_data/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String inputElement) throws Exception {
                        String[] arr = inputElement.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })

                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        stream
                .keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))

                .aggregate(new CountAgg(), new WindowResult())
                //DataStream[ItemViewCount]
                // 对DataStream[ItemViewCount]使用窗口结束时间进行分流
                // 每一条支流里面的元素都属于同一个窗口，元素是ItemViewCount
                // 所以只需要对支流里面的元素按照count字段进行排序就可以了
                // 支流里的元素是有限的，因为都属于同一个窗口
                .keyBy(itemViewCount -> itemViewCount.windowEnd)
                .process(new TopN(3))

                .print();
        env.execute();
    }

    public static class TopN extends KeyedProcessFunction <Long, ItemViewCount, String>{

        //初始化一个列表状态变量
        private ListState<ItemViewCount> itemState;
        private Integer threshold;

        public TopN(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("items", ItemViewCount.class)
            );
        }

        //每来一条ItemViewCount就调用一次
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            // 由于所有value的windowEnd都一样，所以只会注册一次定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd +100l);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> ItemViewCounts = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemState.get()) {
                ItemViewCounts.add(itemViewCount);
            }
            itemState.clear();
            ItemViewCounts
                .sort(new Comparator<ItemViewCount>(){
                    @Override
                    public int compare(ItemViewCount item1, ItemViewCount item2) {
                        return item2.count.intValue() - item1.count.intValue();
                    }
            });
            StringBuilder result = new StringBuilder();
            result
                    .append("=============================================================\n");
            result
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");

            for (int i = 0; i < this.threshold; i++) {
                ItemViewCount item = ItemViewCounts.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(" itemID = ")
                        .append(item.itemId)
                        .append(" count = ")
                        .append(item.count)
                        .append("\n");
            }
            result
                    .append("=============================================================\n\n");
            Thread.sleep(1000L);
            out.collect(result.toString());
        }
    }


    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(key, context.window().getEnd(), iterable.iterator().next()));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }


}
