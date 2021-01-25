package com.as.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> fileDS = env.readTextFile("hdfs://node1:8020/flink_test/wordcount");

       //SingleOutputStreamOperator<Tuple2<String, Integer>> result = CommonUtils.wordCount(fileDS);
        fileDS.print();

        env.execute();
    }
}
