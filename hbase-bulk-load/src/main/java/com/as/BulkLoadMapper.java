package com.as;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private ImmutableBytesWritable writable = new ImmutableBytesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isNotBlank(line)) {
            String[] fields = line.split(",");
            byte[] rowKey = fields[0].getBytes();
            writable.set(rowKey);

            Put put = new Put(rowKey);
            put.addColumn("C1".getBytes(),"code".getBytes(),fields[1].getBytes());
            put.addColumn("C1".getBytes(),"rec_account".getBytes(),fields[2].getBytes());
            put.addColumn("C1".getBytes(),"rec_bank_name".getBytes(),fields[3].getBytes());
            put.addColumn("C1".getBytes(),"rec_name".getBytes(),fields[4].getBytes());
            put.addColumn("C1".getBytes(),"pay_account".getBytes(),fields[5].getBytes());
            put.addColumn("C1".getBytes(),"pay_name".getBytes(),fields[6].getBytes());
            put.addColumn("C1".getBytes(),"pay_comments".getBytes(),fields[7].getBytes());
            put.addColumn("C1".getBytes(),"pay_channel".getBytes(),fields[8].getBytes());
            put.addColumn("C1".getBytes(),"pay_way".getBytes(),fields[9].getBytes());
            put.addColumn("C1".getBytes(),"status".getBytes(),fields[10].getBytes());
            put.addColumn("C1".getBytes(),"timestamp".getBytes(),fields[11].getBytes());
            put.addColumn("C1".getBytes(),"money".getBytes(),fields[12].getBytes());

            context.write(writable, put);

        }
    }
}
