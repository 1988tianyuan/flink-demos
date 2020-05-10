package com.tianyuan.flinkdemos.hotitems;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ItemTopNFunction extends KeyedProcessFunction<Tuple, UserItemCountResult, String> {

    private ListState<UserItemCountResult> itemCountResultListState;

    @Override
    public void processElement(UserItemCountResult countResult, Context ctx, Collector<String> out) throws Exception {
        itemCountResultListState.add(countResult);
        ctx.timerService().registerEventTimeTimer(countResult.windowEnd + 1);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<UserItemCountResult> listStateDescriptor =
                new ListStateDescriptor<>("user-item-count-result", UserItemCountResult.class);
        itemCountResultListState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<UserItemCountResult> countResultList = new ArrayList<>();
        for (UserItemCountResult countResult : itemCountResultListState.get()) {
            countResultList.add(countResult);
        }
        itemCountResultListState.clear();
        countResultList.sort((result1, result2) -> (int)(result2.viewCount - result1.viewCount));
        StringBuilder builder = new StringBuilder("====================================\n");
        builder.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < 3; i++) {
            builder.append("NO.").append(i).append(": ")
                   .append("商品ID=").append(countResultList.get(i).itemId).append(" ")
                   .append("浏览量=").append(countResultList.get(i).viewCount).append("\n");
        }
        builder.append("====================================\n");
        out.collect(builder.toString());
    }
}
