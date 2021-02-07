package cn._miao.hdfs.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * cn._miao.hdfs.mr.wc
 *
 * @Auther: Cosette
 * @Date: 2021/2/6 17:23
 * @Description:
 *  接收maptask端的数据 (a,1) (a,1)  聚合
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 一个单词执行一次 (a,1) (a,1)
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            i++;
        }
        context.write(key, new IntWritable(i));
    }
}
