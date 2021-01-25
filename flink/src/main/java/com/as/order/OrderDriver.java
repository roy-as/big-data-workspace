package com.as.order;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OrderDriver {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Tuple2<String, Double>> order = env.addSource(new OrderSource());

        order.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<Tuple2<String, Double>, Double, Double>() {


                    @Override
                    public Double createAccumulator() {
                        return 0d;
                    }

                    @Override
                    public Double add(Tuple2<String, Double> value, Double accumulator) {
                        return value.f1 + accumulator;
                    }

                    @Override
                    public Double getResult(Double accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        return a + b;
                    }

                }, new WindowFunction<Double, Client.CategoryBean, String, TimeWindow>() {
                    FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<Client.CategoryBean> out) throws Exception {
                        Client.CategoryBean bean = new Client.CategoryBean(key, input.iterator().next(), df.format(System.currentTimeMillis()));
                        out.collect(bean);
                    }
                }).print();

        env.execute();
    }
}
