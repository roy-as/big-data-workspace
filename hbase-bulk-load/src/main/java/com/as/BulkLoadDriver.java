package com.as;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkLoadDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "bulkLoad");
        job.setJarByClass(BulkLoadDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/buckLoad/bank/input"));

        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(HFileOutputFormat2.class);

        Connection connection = ConnectionFactory.createConnection(this.getConf());
        TableName tableName = TableName.valueOf("bank:transfer_record");
        Table table = connection.getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));
        HFileOutputFormat2.setOutputPath(job, new Path("hdfs://node1:8020/bulkLoad/back/output"));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        int flag = ToolRunner.run(conf, new BulkLoadDriver(), args);
        System.exit(flag);
    }
}
