package com.atguigu.day11;

import com.atguigu.day11.util.ApacheLog;
import com.atguigu.day11.util.UrlViewCount;
import com.atguigu.day9.ItemViewCount;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class ApacheLogAnalysis {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //开始连接数据源
        DataStream<ApacheLog> dataStream = env.readTextFile("F:/flink_data/apache.log")
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String value) throws Exception {
                        //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
                        String[] arr = value.split(" ");
                        String aa = arr[0];
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        long time = simpleDateFormat.parse(arr[3]).getTime();
                        return new ApacheLog(arr[0],arr[2],time,arr[5],arr[6]);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ApacheLog>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                            @Override
                            public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })

                );

        dataStream

                .keyBy(r ->r.url)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResult())
                .keyBy(urlViewCount -> urlViewCount.windowEnd)
                .process(new reqTopN(3))
                .print();

        env.execute();

    }


    private static class reqTopN extends KeyedProcessFunction< Long,UrlViewCount,String> {

        //初始化一个列表状态变量
        private ListState<UrlViewCount> itemState;
        private Integer threshold;

        public reqTopN(Integer threshold) {
            this.threshold = threshold;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("urls", UrlViewCount.class)
            );
        }

        //每来一条ItemViewCount就调用一次
        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd +100l);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            List<UrlViewCount> urlViewCounts = new ArrayList<>();
            for (UrlViewCount urlViewCount : itemState.get()) {
                urlViewCounts.add(urlViewCount);
            }
            itemState.clear();
            urlViewCounts
                    .sort(new Comparator<UrlViewCount>(){
                        @Override
                        public int compare(UrlViewCount item1, UrlViewCount item2) {
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
                UrlViewCount urlViewCount = urlViewCounts.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(" url = ")
                        .append(urlViewCount.url)
                        .append(" count = ")
                        .append(urlViewCount.count)
                        .append("\n");
            }
            result
                    .append("=============================================================\n\n");
            Thread.sleep(1000L);
            out.collect(result.toString());



        }
    }


    public static class WindowResult extends ProcessWindowFunction<Long, UrlViewCount,String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(s,context.window().getEnd(),elements.iterator().next()));
        }
    }


    private static class CountAgg implements AggregateFunction<ApacheLog,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0l;
        }
        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator + 1 ;
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
