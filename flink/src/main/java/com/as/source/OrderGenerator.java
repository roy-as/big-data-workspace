package com.as.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class OrderGenerator extends RichParallelSourceFunction<Order> {

    private Boolean flag = true;


    @Override
    public void run(SourceContext<Order> context) throws Exception {
        while (true) {
            String id = UUID.randomUUID().toString().replaceAll("-", "");
            String name = "";
            Double money = Math.random() * 100 + 1;
            Long now = System.currentTimeMillis();
            context.collect(new Order(id, name, money,  now));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
