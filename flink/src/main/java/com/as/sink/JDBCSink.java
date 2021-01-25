package com.as.sink;

import com.as.source.MysqlSource;
import com.as.source.Student;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;

public class JDBCSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setMaxParallelism(1);
        env.setParallelism(1);
        env.enableCheckpointing(1000);


        env.setStateBackend(new FsStateBackend("file:///Users/aby/Desktop/practice/flink/student"));

        DataStream<Student> ds = env.addSource(new StudentSource());

        ds.process(new ProcessFunction<Student, Student>() {

            @Override
            public void processElement(Student value, Context ctx, Collector<Student> out) throws Exception {
                if(value.getId() % 3 == 0) {
                    System.out.println("出bug了");
                    out.collect(value);
                    throw new RuntimeException("出bug了");
                }
                out.collect(value);
            }
        }).print();


//        ds.addSink(JdbcSink.sink(
//                "insert into student values(?, ?, ?)",
//                (ps, student) -> {
//                    ps.setInt(1, student.getId());
//                    ps.setString(2, student.getName());
//                    ps.setInt(3, student.getAge());
//                },
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://node2:3306/test")
//                        .withDriverName("com.mysql.jdbc.Driver")
//                        .withUsername("root")
//                        .withPassword("123456")
//                        .build()
//
//
//        ));
        //ds.addSink(new StudentSink());

        env.execute();
    }

    public static class StudentSource extends RichParallelSourceFunction<Student> {


        @Override
        public void run(SourceContext<Student> ctx) throws Exception {

            for (int i = 1; i <= 1000; i++) {
                String name = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 5);
                int age = (int) (Math.random() * 20 + 10);
                Student student = new Student(i, name, age);
                ctx.collect(student);
                Thread.sleep(2000);

            }

        }

        @Override
        public void cancel() {

        }
    }
    public static class StudentSink extends RichSinkFunction<Student> {
        Connection conn = null;
        PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://node2:3306/test", "root", "123456");
            String sql = "insert into student values(?, ?, ?)";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Student value, Context context) throws Exception {
            System.out.println(value);
            ps.setInt(1, value.getId());
            ps.setString(2, value.getName());
            ps.setInt(3, value.getAge());
            ps.executeUpdate();

        }

        @Override
        public void close() throws Exception {
            if(null != ps) ps.close();
            if(null != conn) conn.close();
        }
    }
}
