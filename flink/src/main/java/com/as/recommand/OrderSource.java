package com.as.recommand;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class OrderSource extends RichParallelSourceFunction<Order> {

    boolean flag = true;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {

        for (int i = 0; i < 100; i++) {
            int userId = (int) (Math.random() * 10 + 1);
            String orderId = UUID.randomUUID().toString().replaceAll("-", "");
            long time = System.currentTimeMillis();
            ctx.collect(new Order(orderId, String.valueOf(userId), time));
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
