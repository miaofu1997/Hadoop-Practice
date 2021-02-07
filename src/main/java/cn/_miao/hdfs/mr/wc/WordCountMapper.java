package cn._miao.hdfs.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName: WordCountMapper
 * Description:
 * 1 在hdp的MapReduce程序中  处理数据是以KV的形式处理  K   V
 * 2 输入数据是  kV    输出数据  KV
 * 3 输入的数据格式 默认是文本数据 (行)
 *    输入 K  行数据的起始偏移量   Long
 *    输入 V  当前行内容         String
 *    输出 K  单词              String
 *    输出 V  个数               int
 *
 *  数据源 -----> maptask产生数据 ---> reducetask 可能不在同一台机器上 需要网络传输数据 ---> KIN VIN KOUT VOUT需要序列化
 *  String  Long  Integer 默认是 Serializable 对于大量数据的传输不利(有冗余)  HDP自己优化了的序列化规则
 *  Long    --->  LongWritable
 *  String  ---> Text
 *  Integer ---> IntWritable
 *  Double  ---> DoubleWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 一行数据执行一次 多少行就执行多少次
     * @param key    起始位置
     * @param value   一行的内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理一行
        String line = value.toString();
        String[] words = line.split("\\s+");
        for (String word : words) {
            // hello tom jim tom cat --> (hello 1) (tom 1) (jim 1) (tom 1) (cat 1)
            // 之后reduce再聚合
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
