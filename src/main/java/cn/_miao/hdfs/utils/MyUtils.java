package cn._miao.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * cn._miao.hdfs.client.utils
 *
 * @Auther: Cosette
 * @Date: 2021/2/2 20:26
 * @Description:
 */
public class MyUtils {
    /**
     * 获取分布式文件系统对象
     * 参数
     * 返回值
     */
    public static FileSystem getFs() throws Exception {
        Configuration conf = new Configuration();
        return FileSystem.newInstance(new URI("hdfs://nn1.hdp:8020"), conf, "root");
    }
}
