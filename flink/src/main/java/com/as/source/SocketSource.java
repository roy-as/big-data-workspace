package com.as.source;

import com.as.common.CommonUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SocketSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<String> socketDS = env.socketTextStream(
                "node1",
                9999
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = CommonUtils.wordCount(socketDS);

        result.print();

        env.execute();
    }
}
