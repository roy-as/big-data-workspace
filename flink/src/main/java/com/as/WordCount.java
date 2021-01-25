package com.as;

import com.as.common.CommonUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds = env.socketTextStream("node1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = CommonUtils.wordCount(ds);

        result.print();
        //env.execute();

        int a = 10;
        int b = 8;

        System.out.println(a ^ b ^ a);
    }
}
