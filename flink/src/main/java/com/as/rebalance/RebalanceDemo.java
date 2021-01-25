package com.as.rebalance;

import com.as.common.CommonUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Long> ds = env.fromSequence(0, 1000);

        SingleOutputStreamOperator<Long> filterDs = ds.filter(num -> num > 100);
        DataStream<Tuple2<String, Long>> result1 = CommonUtils.map(filterDs);

        //result1.print("result1");

        DataStream<Tuple2<String, Long>> result2 = CommonUtils.map(filterDs.rebalance());

        result2.print("result2");

        env.execute();
    }
}
