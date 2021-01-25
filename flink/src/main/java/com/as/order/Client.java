package com.as.order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Client {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Tuple2<String, Double>> orderDs = env.addSource(new OrderSource());

        SingleOutputStreamOperator<CategoryBean> categoryResult = orderDs.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new OrderAggregate(), new OrderWindowFunction());

        //categoryResult.print();

        categoryResult.keyBy(CategoryBean::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new OrderProcessFunction()).print();

        //result.print();

        env.execute();

    }

    public static class OrderAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {

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

    }

    public static class OrderWindowFunction implements WindowFunction<Double, CategoryBean, String, TimeWindow> {

        private FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryBean> out) throws Exception {

            String dateTime = df.format(System.currentTimeMillis());
            CategoryBean bean = new CategoryBean(key, input.iterator().next(), dateTime);
            out.collect(bean);
        }
    }


    public static class OrderProcessFunction extends ProcessWindowFunction<CategoryBean, Object, String, TimeWindow> {

        @Override
        public void process(String dateTime, Context context, Iterable<CategoryBean> elements, Collector<Object> out) throws Exception {
            double total = 0d;
            Iterator<CategoryBean> it = elements.iterator();
            PriorityQueue<CategoryBean> queue = new PriorityQueue<>(3, Comparator.comparing(CategoryBean::getPrice));
            while (it.hasNext()) {
                CategoryBean bean = it.next();
                total += bean.getPrice();
                if(queue.size() < 3) {
                    queue.add(bean);
                } else {
                    if(queue.peek().getPrice() < bean.getPrice()) {
                        queue.poll();
                        queue.add(bean);
                    }
                }
            }
            System.out.println("时间:" + dateTime + ",总金额:" + total);
            System.out.println("前三分类:" + queue);

        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class CategoryBean {

        private String category;

        private Double price;

        private String dateTime;

    }

}
