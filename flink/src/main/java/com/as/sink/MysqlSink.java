package com.as.sink;

import com.as.source.Student;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink {

    public static void main(String[] args) throws Exception {
        Student s1 = new Student(null, "brook", 30);
        Student s2 = new Student(null, "larry", 35);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Student> ds = env.fromElements(s1, s2);


        ds.addSink(new RichSinkFunction<Student>() {

            Connection conn = null;
            PreparedStatement ps = null;

            @Override
            public void open(Configuration parameters) throws Exception {

                conn = DriverManager.getConnection("jdbc:mysql://node2:3306/test", "root", "123456");
                String sql = "insert into student values(null, ?, ?)";
                ps = conn.prepareStatement(sql);

            }

            @Override
            public void invoke(Student value, Context context) throws Exception {
                ps.setString(1, value.getName());
                ps.setInt(2, value.getAge());
                ps.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if(null != ps) {
                    ps.close();
                }
                if(null != conn) {
                    conn.close();
                }

            }
        });

        env.execute();
    }
}
