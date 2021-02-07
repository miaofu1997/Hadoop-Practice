package cn._miao.hdfs.mr.line;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * cn._miao.hdfs.mr.line
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 17:28
 * @Description:
 */
public class LineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count ++;
        }
        context.write(key, new IntWritable(count));
    }
}
