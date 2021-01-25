package com.as.state;

import com.as.common.CommonUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class CheckpointDemo2 {

    /**
     * flink配置了checkpoint默认无限重启恢复状态
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
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

        DataStream<String> ds = env.socketTextStream("node1", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = CommonUtils.wordCount(ds);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node1:9092");

        result.map(t -> {
            if (t.f1 % 3 == 0) {
                System.out.println("出bug了");
                throw new Exception("出bug了");
            }
            return t.f0 + "::" + t.f1;
        }).returns(Types.STRING)
        .addSink(new FlinkKafkaProducer<String>("event", new SimpleStringSchema(), prop));
        env.execute();


    }
}
