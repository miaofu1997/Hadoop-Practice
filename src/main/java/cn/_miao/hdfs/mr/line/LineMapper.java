package cn._miao.hdfs.mr.line;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * cn._miao.hdfs.mr.line
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 17:28
 * @Description:
 */
public class LineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        int x1 = Integer.parseInt(split[0]); // 起点坐标 1
        int x2 = Integer.parseInt(split[1]); // 终点坐标 4
        for (int i = x1; i <= x2; i++) { // 1 2 3 4
            context.write(new Text(i + ""), new IntWritable(1));
        }
    }
}
