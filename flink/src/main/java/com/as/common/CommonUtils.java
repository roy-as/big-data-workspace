package com.as.common;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

public class CommonUtils {

    public static SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount(DataStream<String> dataStream) {
        return dataStream.flatMap((String line, Collector<Tuple2<String, Integer>> collector)
                -> Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1)))
        ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1);
    }

    public static DataStream<Tuple2<String, Long>> map(DataStream<Long> ds) {

        DataStream<Tuple2<String, Long>> mapDs = ds.map(new RichMapFunction<Long, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(Long value) throws Exception {
                RuntimeContext context = this.getRuntimeContext();
                int index = context.getIndexOfThisSubtask();
                return Tuple2.of("分区:" + index, 1L);
            }
        });
        return mapDs.keyBy(t -> t.f0).reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));
    }

    private static Properties props;

    static {
        props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交
    }

    public static Properties getKafkaConf() {
        return props;
    }

    public static StreamExecutionEnvironment getEnv(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // checkpoint周期
        env.enableCheckpointing(1000);

        // 设置checkpoint方式
        //memoryStateBackend state和checkpoint都在内存
        //fsStateBackend state在内存，checkpoint在存储介质(磁盘/hdfs）
        //rocksDBState state在内存+磁盘上，checkpoint在存储介质上，一般用于state量比较大的情况
        //state是指某个operator运行时的状态，checkpoint是指所有operator在某个时刻state的快照
        env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink_checkpoint/checkpoint"));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 同一个checkpoint最小的间隔时间，默认为0。避免上一个checkpoint还没
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 允许checkpoint失败的次数
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 作业取消后是否保留外部的checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 默认为exact_once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint超时时间，超过了就认为checkpoint失败
        checkpointConfig.setCheckpointTimeout(60000);
        // 最大可同时执行多少个checkpoint，默认为1
        //checkpointConfig.setMaxConcurrentCheckpoints(1);

        //配置checkpoint默认无限重启恢复状态
        //env.setRestartStrategy(RestartStrategies.noRestart());// 出错不重启

        //env.setRestartStrategy(RestartStrategies.fallBackRestart());// 默认

        //env.setRestartStrategy(
        //        RestartStrategies.fixedDelayRestart(
        //                3, 10 * 1000L)
        //);// 第一个参数代表重启次数，重启次数大于他时就不再重启，第二个参数重启间隔时间

        //env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //        3, // 每个测量阶段失败的最大失败数
        //        Time.minutes(1), // 失败测量时间间隔
        //        Time.seconds(10) // 重启时间间隔
        //));
        return env;
    }
}
