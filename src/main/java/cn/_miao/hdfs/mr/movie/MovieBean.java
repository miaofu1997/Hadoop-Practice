package cn._miao.hdfs.mr.movie;

/**
 * cn._miao.hdfs.mr.movie
 *
 * @Auther: Cosette
 * @Date: 2021/2/9 19:37
 * @Description:
 */
public class MovieBean {
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
}
