package com.as.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class NumberSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds = env.addSource(new NumSource());

       ds.writeAsText("./file/file2").setParallelism(1);

       env.execute();


    }


    public static class NumSource extends RichParallelSourceFunction<String> {

        boolean flag = true;

        @Override
        public void run(SourceContext<String> context) throws Exception {
            while (flag) {
                int num = (int) (Math.random() * 20);
                int value = (int) (Math.random() * 100);
                context.collect(num + "," + value);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
