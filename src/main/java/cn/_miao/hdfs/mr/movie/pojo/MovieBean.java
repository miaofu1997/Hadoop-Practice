package cn._miao.hdfs.mr.movie.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * cn._miao.hdfs.mr.movie
 *
 * @Auther: Cosette
 * @Date: 2021/2/9 19:37
 * @Description:
 * 1 电影的总分
 * 2 均分
 * 3 每部电影 top
 * 4 每部电影的评论次数
 * 5 评论次数topN  热门电影
 *   序列化
 *   MovieBean是用来 封装数据的
 *   mysql的表数据
 *   行数据....
 * 只有 get  set  toStirng
 * 称之为javaBean = pojo = domain
 * pojo (Plain Ordinary Java Object)
 */
public class MovieBean implements Writable {
    //{"movie":"590","rate":"4","timeStamp":"978297439","uid":"3"}
    private String  movie ;
    private double  rate ;
    private String timeStamp ;
    private String  uid ;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "MovieBean{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }

    /**
     * 序列化的方法  写出去  属性
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeDouble(rate);
        dataOutput.writeUTF(timeStamp);
        dataOutput.writeUTF(uid);
    }

    /**
     * 反序列化   读回来  读属性
     * @param dataInput
     * @throws IOException
     * 注意读取的属性的 个数 顺序 和数据类型
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movie =  dataInput.readUTF();
        rate = dataInput.readDouble();
        timeStamp =  dataInput.readUTF();
        uid =  dataInput.readUTF();
    }
}
