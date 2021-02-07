package cn._miao.hdfs.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * cn._miao.hdfs.mr.wc
 *
 * @Auther: Cosette
 * @Date: 2021/2/6 17:18
 * @Description: 使用mr程序统计文件中单词出现的次数
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1  任务   统计文件中单词出现的次数
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        // 怎么处理数据?
        // 2 设置reduce类  mapper类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 3 设置map和reduce端的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //4设置reduce的数量
        job.setNumReduceTasks(2);

        // 处理数据的输入, 处理什么数据?
        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Desktop/Bigdata/word.txt"));

        // 结果输出, 结果怎么输出?
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Desktop/Bigdata/word_res"));

        //提交任务
        job.waitForCompletion(true);

    }
}
