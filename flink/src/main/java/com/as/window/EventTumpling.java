package com.as.window;

import com.as.common.CommonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 滚动事件事件，watermark是最大事件事件-允许延迟事件
 * 窗口计算通过watermark来触发的
 * 1.窗口中有数据
 * 2.watermark > 窗口结束时间
 */
public class EventTumpling {

    static FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss.sss");


    /**
     * 测试数据
     * 1,100,2021-01-17 10:11:11
     * 1,200,2021-01-17 10:11:09
     * 1,200,2021-01-17 10:11:15
     * 1,700,2021-01-17 10:11:08
     * 1,700,2021-01-17 10:11:21
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

//        DataStream<Money> ds = env.addSource(new RichParallelSourceFunction<Money>() {
//            boolean flag = true;
//
//            @Override
//            public void run(SourceContext<Money> context) throws Exception {
//                for (int i = 20; i >= 1; i--) {
//                    long eventTime = System.currentTimeMillis() - i * 2000;
////                   if(i < 15) {
////                       eventTime = (long) (Math.random() * 20000);
////                   }else {
////                       eventTime = (long) (Math.random() * 3000);
////                   }
//                    int money = (int) (Math.random() * 5 + 1);
//                    context.collect(new Money(1, 1, eventTime));
//                    System.out.println(MessageFormat.format(
//                            "发送数据:id:{0},num:{1}, 事件时间:{2},当前时间:{3}",
//                            1,
//                            money,
//                            df.format(eventTime),
//                            df.format(System.currentTimeMillis()))
//                    );
//                    Thread.sleep(1000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//                flag = false;
//            }
//        }).setParallelism(1);


        // DataStream<String> ds = env.socketTextStream("node1", 9999);


        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("file:///Users/aby/Desktop/practice/flink/check"));


        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("event", new SimpleStringSchema(), CommonUtils.getKafkaConf());

        SingleOutputStreamOperator<Money> kafkaDs = env.addSource(kafkaSource).map(line -> {
            String[] data = line.split(",");
            if(data[0].equals("2")) {
                System.out.println("出bug了");
                throw new Exception("出bug了");
            }
            Money money = new Money(Integer.parseInt(data[0]), Integer.parseInt(data[1]), Long.parseLong(data[2]));
            System.out.println(MessageFormat.format(
                    "发送数据:id:{0},num:{1}, 事件时间:{2},当前时间:{3}",
                    money.id,
                    money.num,
                    df.format(money.eventTime),
                    df.format(System.currentTimeMillis()))
            );
            return money;
        });
        SingleOutputStreamOperator<String> result = kafkaDs
                .map(money -> {
                    System.out.println(MessageFormat.format(
                            "map:(id:{0},num:{1}, 事件时间:{2},当前时间:{3})",
                            money.id,
                            money.num,
                            df.format(money.eventTime),
                            df.format(System.currentTimeMillis()))
                    );
                    return money;
                })
                // 指定最大延迟事件
                .assignTimestampsAndWatermarks
                        (WatermarkStrategy.<Money>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                // 指定事件时间列
                                .withTimestampAssigner((money, timestamp) -> money.getEventTime())
                        )
                .keyBy(Money::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Money, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Money> moneys, Collector<String> out) throws Exception {
                        //用来存放当前窗口的数据的格式化后的事件时间
                        List<String> list = new ArrayList<>();
                        for (Money money : moneys) {
                            Long eventTime = money.eventTime;
                            list.add(df.format(eventTime));
                        }
                        String start = df.format(window.getStart());
                        String end = df.format(window.getEnd());
                        //现在就已经获取到了当前窗口的开始和结束时间,以及属于该窗口的所有数据的事件时间,把这些拼接并返回
                        String outStr = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s,当前时间:%s", key.toString(), start, end, list.toString(), df.format(System.currentTimeMillis()));
                        //System.out.println(outStr);
                        out.collect(outStr);
                    }
                });

        result.print();

        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Money {

        private Integer id;

        private Integer num;

        private Long eventTime;

    }
}

