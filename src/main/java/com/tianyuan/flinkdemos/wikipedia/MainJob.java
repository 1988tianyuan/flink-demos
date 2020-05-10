package com.tianyuan.flinkdemos.wikipedia;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class MainJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new WikipediaEditsSource())
                .keyBy(WikipediaEditEvent::getUser)
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple3<String, Integer, StringBuilder>,
                        Tuple3<String, Integer, StringBuilder>>() {
                    @Override
                    public Tuple3<String, Integer, StringBuilder> createAccumulator() {
                        return new Tuple3<>("", 0, new StringBuilder());
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> add(WikipediaEditEvent editEvent,
                                                                      Tuple3<String, Integer, StringBuilder> tuple3) {
                        StringBuilder sb = tuple3.f2;
                        if (StringUtils.isBlank(sb.toString())) {
                            sb.append("Details: ");
                        } else {
                            sb.append(" ");
                        }
                        return new Tuple3<>(editEvent.getUser(), editEvent.getByteDiff(), sb.append(editEvent.getByteDiff()));
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> getResult(Tuple3<String, Integer, StringBuilder> tuple3) {
                        return tuple3;
                    }

                    @Override
                    public Tuple3<String, Integer, StringBuilder> merge(Tuple3<String, Integer, StringBuilder> tuple3,
                                                                        Tuple3<String, Integer, StringBuilder> acc1) {
                        return new Tuple3<>(tuple3.f0, tuple3.f1 + acc1.f1, tuple3.f2.append(acc1.f2));
                    }
                })
                .map(Tuple3::toString)
                .print();
        env.execute("Flink wikipedis demo.");
    }
}
