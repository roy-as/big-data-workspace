package com.as.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds = env.readTextFile("./file/file1");

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = ds.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
            Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1)));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

        result.print();
        env.execute();


    }
}
