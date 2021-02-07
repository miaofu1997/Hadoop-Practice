package cn._miao.hdfs.mr.flow;

import cn._miao.hdfs.mr.line.Line;
import cn._miao.hdfs.mr.line.LineMapper;
import cn._miao.hdfs.mr.line.LineReducer;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * cn._miao.hdfs.mr.flow
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 20:50
 * @Description:
 * 1 在map方法中代码处理异常 catch 最大异常
 * 2 map方法一行执行一次 不要在map方法中new对象 防止重复创建
 * 3 静态的内部类
 * 4 map端和reduce端的输出一致的时候 可以省略  setMapOutputKeyClass  setMapOutputvalueClass
 */
public class Flow {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Flow.class.getSimpleName());

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        /**
         * 当map端和reduce端的输出一致的时候  可以省略
         */
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/flow/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/flow/res"));

        job.waitForCompletion(true);

    }

    static class FlowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Text k = new Text();
        LongWritable v = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] split = line.split("\\s+");
                String phone = split[1];
                long upFlow = Long.parseLong(split[split.length - 3]);
                long downFlow = Long.parseLong(split[split.length - 2]);
                long flow = upFlow + downFlow;
                k.set(phone);
                v.set(flow);
                context.write(k, v);
            } catch (Exception e) { //
                e.printStackTrace();
            }

        }
    }

    static class FlowReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        LongWritable v = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalFlow = 0L;
            for (LongWritable value : values) {
                totalFlow += value.get();
            }
            v.set(totalFlow);
            context.write(key, v);
        }
    }
}
