package com.as.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BatchWordCount {


    public static void main(String[] args) throws Exception {
        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> ds = env.fromElements("spark hive hbase",
                "mr hadoop hdfs",
                "hdfs hive hbase spark", "yarn yarn spark hive");

        DataSet<Tuple2<String, Integer>> result = ds.flatMap((String line, Collector<String> collector) -> {
            String[] words = line.split(" ");
            System.out.println(Arrays.toString(words));
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING)
                .map((String word) -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1);
        result.print();

        //env.execute();
    }
}
