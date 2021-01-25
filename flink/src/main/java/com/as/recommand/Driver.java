package com.as.recommand;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class Driver {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Order> ds = env.addSource(new OrderSource());

        ds.keyBy(Order::getUserId)
                .process(new TimerProcess())
                .print("自动好评订单");

        env.execute();


    }

    public static class TimerProcess extends KeyedProcessFunction<String, Order, Object> {
        long timeOut = 5000L;
        MapState<String, Order> state = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Order> stateDescriptor = new MapStateDescriptor<>(
                    "state", TypeInformation.of(String.class), TypeInformation.of(Order.class));
            state = this.getRuntimeContext().getMapState(stateDescriptor);

        }

        @Override
        public void processElement(Order order, Context ctx, Collector<Object> out) throws Exception {
            state.put(order.getOrderId(), order);
            ctx.timerService().registerEventTimeTimer(order.getFinishTime() + timeOut);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            Iterable<String> keys = state.keys();
            Iterator<Map.Entry<String, Order>> it = state.iterator();
            while (it.hasNext()) {
                Map.Entry<String, Order> order = it.next();
                if (this.isCommend(order.getKey())) {
                    it.remove();
                    System.out.println("订单:" + order.getKey() + "已评价");
                } else {
                    if (System.currentTimeMillis() >= order.getValue().getFinishTime() + timeOut) {
                        System.out.println("订单:" + order.getKey() + "超时，自动好评");
                        it.remove();
                        out.collect(order.getValue());
                    }
                }
            }
        }

        public boolean isCommend(String orderId) {
            return orderId.hashCode() % 3 == 0;
        }
    }

}
