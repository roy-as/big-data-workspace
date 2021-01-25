package com.as.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlSource extends RichParallelSourceFunction<Student> {

    private Connection conn = null;

    private PreparedStatement ps = null;

    private Boolean flag = true;

    private ResultSet rs = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://node2:3306/test", "root", "123456");
        String sql = "select id, name, age from student";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Student> context) throws Exception {
        while (flag) {
            Thread.sleep(5000);
            rs = ps.executeQuery();
            int num = 0;
            while (rs.next()) {

                Student student = new Student(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                );
                context.collect(student);
                num++;
            }
            System.out.println("数量为：" + num);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        if (null != rs) {
            rs.close();
        }
        if (null != ps) {
            ps.close();
        }

        if (null != conn) {
            conn.close();
        }
    }
}
