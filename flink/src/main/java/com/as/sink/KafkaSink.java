package com.as.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092");
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("flink.partition-discovery.interval-millis","5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "kafka_source",
                new SimpleStringSchema(),
                props

        );
        DataStreamSource<String> ds = env.addSource(kafkaSource);
        SingleOutputStreamOperator<String> result = ds.filter(word -> word.contains("success"));

        result.print();

        result.addSink(new FlinkKafkaProducer<String>("kafka_sink", new SimpleStringSchema(), props));

        env.execute();
    }
}
