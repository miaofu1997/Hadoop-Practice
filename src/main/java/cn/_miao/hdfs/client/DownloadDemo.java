package cn._miao.hdfs.client;

import cn._miao.hdfs.utils.MyUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * cn._miao.hdfs.client
 *
 * @Auther: Cosette
 * @Date: 2021/2/2 20:40
 * @Description:
 * 将分布式文件系统中的文件 下载到本地
 * 注意: 需要在本地配置HDP的环境变量
 * HADOOP_HOME
 */
public class DownloadDemo {
    public static void main(String[] args) throws Exception {
        FileSystem fs = MyUtils.getFs();
        /**
         * 参数1 是否删除要下载的 默认false
         * 参数2 待上传的文件(可以是数组)
         * 参数3 下载下来的本地系统路径
         * 参数4 本地校验 默认false 用HDFS校验
         */
        fs.copyToLocalFile(false, new Path("/user.txt"), new Path("/Users/cosette/Desktop/bigdata/user.doc")); //下载的同时还改了个名字
        fs.close();
    }
}
