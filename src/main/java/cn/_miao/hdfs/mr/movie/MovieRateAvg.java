package cn._miao.hdfs.mr.movie;

import cn._miao.hdfs.mr.movie.pojo.MovieBean;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * cn._miao.hdfs.mr.movie
 *
 * @Auther: Cosette
 * @Date: 2021/2/7 22:33
 * @Description:
 *  计算电影的平均分
 *  输出结果按照电影的key进行排序
 */
public class MovieRateAvg {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MovieRateAvg.class.getSimpleName());
        job.setMapperClass(MovieRateAvgMapper.class);
        job.setReducerClass(MovieRateAvgReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
//        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/movie/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/movie/res"));
        boolean b = job.waitForCompletion(true);
        if (b) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    static class MovieRateAvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Text k = new Text();
        DoubleWritable v = new DoubleWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                MovieBean mb = JSON.parseObject(line, MovieBean.class);
                String movie = mb.getMovie();
                double rate = mb.getRate();
                k.set(movie);
                v.set(rate);
                context.write(k, v);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class MovieRateAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        /**
         * 一个电影执行一次
         * key  电影
         * value 迭代器  存的当前电影所有的评分
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        DoubleWritable v = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalRate = 0d;
            int count = 0;
            for (DoubleWritable value : values) {
                totalRate += value.get();
                count++;
            }
            double avgRate = totalRate/count;
            v.set(avgRate);
            context.write(key, v);
        }
    }

}
