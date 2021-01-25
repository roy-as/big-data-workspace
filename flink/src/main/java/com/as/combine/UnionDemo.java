package com.as.combine;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class UnionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "hive");

        DataStream<String> ds2 = env.fromElements("es", "hdfs", "yarn");

        DataStream<Integer> ds3 = env.fromElements(1, 2, 3);

        DataStream<String> result1 = ds1.union(ds2);
        result1.print("result1");

        ConnectedStreams<String, String> result2 = ds1.connect(ds2);

        result2.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s;
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        }).print("result2");

        ds1.connect(ds3).map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "string:" + s;
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return "integer:" + integer;
            }
        }).print("result3");

        env.execute();


    }
}
