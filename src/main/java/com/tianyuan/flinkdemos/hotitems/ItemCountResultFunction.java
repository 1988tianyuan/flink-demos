package com.tianyuan.flinkdemos.hotitems;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ItemCountResultFunction implements WindowFunction<Long, UserItemCountResult, Tuple, TimeWindow> {
    @Override
    @SuppressWarnings("unchecked")
    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UserItemCountResult> collector) throws Exception {
        Long itemId = ((Tuple1<Long>)key).f0;
        Long count = iterable.iterator().next();
        collector.collect(new UserItemCountResult(itemId, timeWindow.getEnd(), count));
    }
}
