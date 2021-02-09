package cn._miao.hdfs.mr.movie;

import cn._miao.hdfs.mr.movie.pojo.MovieBean;
import com.google.gson.Gson;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * cn._miao.hdfs.mr.movie
 *
 * @Auther: Cosette
 * @Date: 2021/2/9 21:03
 * Description: 求每部电影评分最高的前n条记录
 * key: movie  value: MovieBean
 * 自定义的javabean:MovieBean 在map端输出的value的位置 要实现HDP的序列化(体现在MovieBean里面的write和read方法)
 * 1   自定义的javabean序列化
 * 2   list集合的排序
 * 3   关于迭代器中数据  存储在list中  一个对象问题
 * 4   遍历 list的长度  Math.min  Math.max
 * 5   非int排序  包装类的排序方法
 * 6   最终输出结果可以只要一个KEY  VALUE  NullWritable
 */
public class MovieRateTopN {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MovieRateTopN.class.getSimpleName());
        job.setMapperClass(MovieRateTopNMapper.class);
        job.setReducerClass(MovieRateTopNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);
//        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/movie/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Desktop/Bigdata/DOIT19-Hadoop/DOIT19-DAY06-HDP03/mrdata/movie/res3"));
        boolean b = job.waitForCompletion(true);
        if (b) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    static class MovieRateTopNMapper extends Mapper<LongWritable, Text, Text, MovieBean> {
        Gson gs = new Gson();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                // 对json数据进行解析  返回pojo类(javaBean)
                MovieBean mb = gs.fromJson(line, MovieBean.class);
                String movie = mb.getMovie();
                k.set(movie);
                context.write(k, mb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 输出结果 就是电影bean 重写的toString结果
     */
    static class MovieRateTopNReducer extends Reducer<Text, MovieBean, MovieBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            try {
                List<MovieBean> list = new ArrayList<MovieBean>();
                // 遍历每部电影的所有的评论  将数据去除放在list中
                for (MovieBean value : values) {
                    MovieBean newmb = new MovieBean();
                    // 复制属性   参数一 新的类  参数二 数据源bean
                    BeanUtils.copyProperties(newmb,value);
                    list.add(newmb);
                }

                // 排序
                list.sort(new Comparator<MovieBean>() {
                    @Override
                    public int compare(MovieBean o1, MovieBean o2) {
                        return Double.compare(o2.getRate(), o1.getRate());
                    }
                });

                // 输出前3
                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    MovieBean res = list.get(i);
                    context.write(res, NullWritable.get());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
