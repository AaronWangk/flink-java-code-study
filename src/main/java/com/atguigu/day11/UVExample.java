package com.atguigu.day11;

import com.atguigu.day9.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

public class UVExample {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehavior> stream = env
                .readTextFile("F:/flink_data/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })

                .filter(r -> r.behavior.equals("pv")) //过滤PV
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        //升序时间戳
                                        return userBehavior.timestamp;
                                    }
                                })
                );

        stream
                // 针对主流直接开窗口，计算每个小时的pv
                // 分流开窗聚合
                .map(new MapFunction<UserBehavior, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("key", value.userId);
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult()).print();



        env.execute();
    }


    //<IN, OUT, KEY, W extends Window>

    public static class WindowResult extends ProcessWindowFunction<Long,String,String,TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long end = context.window().getEnd();
            // 将时间戳转换为LocalDateTime对象
            LocalDateTime dateTime = Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault()).toLocalDateTime();
            // 定义日期格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
            // 格式化日期为字符串
            String formattedDate = dateTime.format(formatter);

            out.collect("window end: " + formattedDate + " uv count: " + elements.iterator().next());
        }
    }




    public static class CountAgg implements AggregateFunction<Tuple2<String,String>,Tuple2< Set<String>,Long>, Long> {
        //todo 累加存在问题

        @Override
        public Tuple2<Set<String>, Long> createAccumulator() {
            Set<String> set = new HashSet<>();
            Long count = 0L;
            return Tuple2.of(set, count);
        }

        @Override
        public Tuple2<Set<String>, Long> add(Tuple2<String, String> in, Tuple2<Set<String>, Long> accumulator) {
            String f1 = in.f1;
            if (!accumulator.f0.contains(f1)) {
                accumulator.f0.add(f1);
                accumulator.f1 += 1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Set<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<Set<String>, Long> merge(Tuple2<Set<String>, Long> a, Tuple2<Set<String>, Long> b) {
            return a;
        }
    }
}
