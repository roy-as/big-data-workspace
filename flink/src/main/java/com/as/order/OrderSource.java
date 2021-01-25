package com.as.order;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class OrderSource implements SourceFunction<Tuple2<String, Double>> {

    boolean flag = true;

    private String[] categorys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};


    @Override
    public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {

        while (flag) {
            int index = (int) (Math.random() * categorys.length);
            String category = categorys[index];
            double price = Math.random() * 100 + 1;
            ctx.collect(Tuple2.of(category, price));
            Thread.sleep(50);
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
