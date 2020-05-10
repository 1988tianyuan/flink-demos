package com.tianyuan.flinkdemos.hotitems;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

public class HotItemsJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // UserBehavior.csv 的本地文件路径
        URL fileUrl = HotItemsJob.class.getClassLoader().getResource("UserBehavior.csv");
        if (fileUrl != null) {
            Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
            PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
            String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
            PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

            DataStream<UserItemCountResult> itemCountStream = env.createInput(csvInput, pojoType)
               .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior userBehavior) {
                    return userBehavior.timestamp * 1000;
                }
            }).filter((FilterFunction<UserBehavior>) userBehavior -> "pv".equals(userBehavior.behavior))
              .keyBy("itemId")
              .timeWindow(Time.minutes(60), Time.minutes(5))   // 每1分钟统计一次前1小时各个商品点击量
              .aggregate(new UserCountAggr(), new ItemCountResultFunction());
            itemCountStream
                .keyBy("windowEnd")
                .process(new ItemTopNFunction())
                .print();
            env.execute("User item counts TopN job");
        }
    }
}
