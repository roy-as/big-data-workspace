package com.as.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String output = "hdfs://node1:8020/flink_test/wordcount";
        if (parameter.has("output")) {
            output = parameter.get("output");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds = env.fromElements("spark hive hbase",
                "mr hadoop hdfs",
                "hdfs hive hbase spark", "yarn yarn spark hive");
        //DataStream<String> ds = env.socketTextStream("node1", 9999);

        DataStream<Tuple2<String, Integer>> result = ds.flatMap(
                (String value, Collector<String> collector) -> Arrays.stream(
                        value.split(" ")).forEach(collector::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1);

        System.setProperty("HADOOP_USER_NAME", "root");

        result.writeAsText(output);

        env.execute("streaming word count");

    }
}
