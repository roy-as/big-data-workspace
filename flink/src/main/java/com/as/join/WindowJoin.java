package com.as.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Good> goodDs = env.addSource(new GoodSource());

        DataStream<Order> orderDs = env.addSource(new OrderSource());


        SingleOutputStreamOperator<Good> goodWatermark = goodDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Good>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((order, timestamp) -> System.currentTimeMillis())
        );

        SingleOutputStreamOperator<Order> orderWatermark = orderDs.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                Duration.ofSeconds(0))
                .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
        );

        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");


        DataStream<OrderDetail> result = goodDs.join(orderDs)
                .where(Good::getGoodId)
                .equalTo(Order::getGoodId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply((JoinFunction<Good, Order, OrderDetail>) (good, order) -> {
                            System.out.println("order:" + order + ",good:" + good);
                            return new OrderDetail(order.orderId, good.goodId, good.name, order.count,
                                    good.price, order.count * good.price, df.format(order.getCreateTime()));

                        }
                );

        result.print();

        env.execute();

    }


    public static class OrderSource implements SourceFunction<Order> {

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {

            while (true) {
                Good good = Good.getInstance();
                Order order = new Order();
                order.setOrderId(UUID.randomUUID().toString().replaceAll("-", ""));
                order.setGoodId(good.goodId);
                order.setCount((int) (Math.random() * 10 + 1));
                order.setCreateTime(System.currentTimeMillis() - 1500);
                ctx.collect(order);

                order.setOrderId(UUID.randomUUID().toString().replaceAll("-", ""));
                order.setGoodId((int) (Math.random() * 100 + 11));
                ctx.collect(order);

                Thread.sleep(1000);

            }

        }

        @Override
        public void cancel() {

        }
    }

    public static class GoodSource implements SourceFunction<Good> {

        @Override
        public void run(SourceContext<Good> ctx) throws Exception {
            while (true) {
                Good.goods.forEach(ctx::collect);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    }


    @Data
    @AllArgsConstructor
    @ToString
    @NoArgsConstructor
    public static class Order {

        private String orderId;

        private int goodId;

        private int count;

        private long createTime;

    }

    @Data
    @AllArgsConstructor
    @ToString
    public static class Good {

        private int goodId;

        private String name;

        private double price;

        static List<Good> goods;

        static {
            goods = new ArrayList<>();
            goods.add(new Good(1, "ihpone12", 8888));
            goods.add(new Good(2, "mate40", 7777));
            goods.add(new Good(3, "note20", 6666));
            goods.add(new Good(4, "小米11", 5555));
            goods.add(new Good(5, "一加9", 4444));
            goods.add(new Good(6, "vivo", 6754));
            goods.add(new Good(7, "oppo", 2342));
            goods.add(new Good(8, "galaxy20", 6565));
            goods.add(new Good(9, "sony", 5434));
            goods.add(new Good(10, "sharp", 3324));
            goods.add(new Good(11, "nokia", 6789));
        }

        public static Good getInstance() {
            int index = (int) (Math.random() * goods.size());
            return goods.get(index);
        }
    }

    @Data
    @AllArgsConstructor
    @ToString
    public static class OrderDetail {

        private String orderId;

        private int goodId;

        private String name;

        private int count;

        private double price;

        private double totalPrice;

        private String time;
    }
}
