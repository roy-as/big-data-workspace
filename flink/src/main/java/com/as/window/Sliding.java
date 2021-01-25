package com.as.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 滑动窗口
 */
public class Sliding {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds = env.socketTextStream("node1", 9999);

        ds.map(line -> {
            String[] data = line.split(",");
            return new CartInfo(Integer.valueOf(data[0]), Integer.valueOf(data[1]));
        }).keyBy(CartInfo::getCartId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count")
                .print();
        env.execute();
    }
}
