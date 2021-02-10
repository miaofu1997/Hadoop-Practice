package cn._miao.hdfs.mr.movie;

import cn._miao.hdfs.mr.movie.pojo.MovieBean;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * cn._miao.hdfs.mr.movie
 *
 * @Auther: Cosette
 * @Date: 2021/2/10 16:14
 * @Description:
 */
public class HotTopN {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, HotTopN.class.getSimpleName());
        job.setMapperClass(HotTopNMapper.class);
        job.setReducerClass(HotTopNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path("/Users/cosette/Bigdata/mrdata/movie/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/cosette/Bigdata/mrdata/movie/hotN_res1"));
        boolean b = job.waitForCompletion(true);
        if (b) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    static class HotTopNMapper extends Mapper<LongWritable, Text, Text, MovieBean> {
        Gson gs = new Gson();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                MovieBean mb = gs.fromJson(line, MovieBean.class);
                k.set(mb.getMovie());
                context.write(k, mb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class HotTopNReducer extends Reducer<Text, MovieBean, Text, IntWritable>{
        Map<String ,Integer> map =  new HashMap<>();
        @Override // 一个电影 每个电影执行一次reduce方法
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (MovieBean value : values) {
               count++;
            }
            // 给每部电影保存它的评论次数
            map.put(key.toString(), count);
        }

        /**
         * 在reduce方法执行完毕之后 最终执行一次 在外面对map进行排序
         * 点进继承的Reducer看到有run方法 每个reduce的最后执行cleanup方法 (如果设置两台reducer 就会有两个文件放各自top3电影)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            /**
             * Set<Entry<T,V>> entrySet()
             * 该方法返回值就是这个map中各个键值对映射关系的集合, 可使用它对map进行遍历
             */
            Set<Map.Entry<String, Integer>> set = map.entrySet();
            //只会list排序 把set放进list方便按照value进行排序
            ArrayList<Map.Entry<String, Integer>> ls = new ArrayList<>(set);
            Collections.sort(ls, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });

            //输出
            for (int i = 0; i < Math.min(3, ls.size()); i++) {
                context.write(new Text(ls.get(i).getKey()), new IntWritable(ls.get(i).getValue()));
            }

        }
    }
}
