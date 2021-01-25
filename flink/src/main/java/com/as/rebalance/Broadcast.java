package com.as.rebalance;

import com.as.common.CommonUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Broadcast {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Long> ds = env.fromSequence(0, 100);

        DataStream<Long> repatition = ds.filter(num -> num > 10).partitionCustom((Partitioner<Long>) (key, i) -> {
            if (key > 10 && key < 50) {
                return 0;
            } else {
                return 1;
            }
        }, (KeySelector<Long, Long>) num -> num);

        //CommonUtils.map(repatition).print("partitionCustom");
        //broadcast将所有数据发送到每一个分区
        //CommonUtils.map( ds.broadcast()).print();
        // 将所有数据发送到第一个分区
        // CommonUtils.map(ds.global()).print();
        //CommonUtils.map(ds.rescale()).print();
        // 随机分配
        //CommonUtils.map(ds.shuffle()).print();
        CommonUtils.map(ds.forward()).print();

        env.execute();

    }
}
