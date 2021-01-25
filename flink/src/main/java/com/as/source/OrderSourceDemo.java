package com.as.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<Order> ds = env.addSource(new OrderGenerator());

        SingleOutputStreamOperator<Tuple2<Integer, Order>> result = ds.map(new RichMapFunction<Order, Tuple2<Integer, Order>>() {
            @Override
            public Tuple2<Integer, Order> map(Order order) throws Exception {

                int index = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(index, order);
            }
        }).setParallelism(2);

        result.print();

        env.execute();

    }
}
