package com.as.split;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Long> ds = env.fromSequence(1, 100);


        OutputTag<Long> odd = new OutputTag<>("odd", TypeInformation.of(Long.class));
        OutputTag<Long> even = new OutputTag<>("even", TypeInformation.of(Long.class));

        SingleOutputStreamOperator<Long> result = ds.process(new ProcessFunction<Long, Long>() {

            @Override
            public void processElement(Long num, Context context, Collector<Long> collector) throws Exception {
                if (num % 2 == 0) {
                    context.output(even, num);
                } else {
                    context.output(odd, num);
                }
            }
        });
        DataStream<Long> so1 = result.getSideOutput(odd);
        so1.print("奇数");

        DataStream<Long> so2 = result.getSideOutput(even);
        so2.print("偶数");

        OutputTag<Long> odd1 = new OutputTag<>("odd1", TypeInformation.of(Long.class));
        OutputTag<Long> even1 = new OutputTag<>("even1", TypeInformation.of(Long.class));

        SingleOutputStreamOperator<Long> result1 = ds.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long num, Context context, Collector<Long> collector) throws Exception {
                if (num % 2 == 0) {
                    context.output(even, num);
                } else {
                    context.output(odd, num);
                }
            }
        });

        result1.getSideOutput(odd1).print("odd1");

        result1.getSideOutput(even1).print("even1");

        env.execute();

    }
}
