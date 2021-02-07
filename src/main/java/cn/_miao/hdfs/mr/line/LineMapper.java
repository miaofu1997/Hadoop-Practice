package cn._miao.hdfs.mr.line;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * cn._miao.hdfs.mr.line
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 17:28
 * @Description:
 */
public class LineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
}
