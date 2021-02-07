package cn._miao.hdfs.mr.line;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.io.Text;

/**
 * cn._miao.hdfs.mr.line
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 17:27
 * @Description:
 */
public class Line {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Line.class.getSimpleName());

        job.setMapperClass(LineMapper.class);
        job.setReducerClass(LineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/line/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/line/res2"));

        boolean b = job.waitForCompletion(true);
    }
}
