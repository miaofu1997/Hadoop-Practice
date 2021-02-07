package cn._miao.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 * cn._miao.hdfs.client
 *
 * @Auther: Cosette
 * @Date: 2021/2/2 20:04
 * @Description:
 *  使用Java操作HDFS分布式文件系统
 *  1) hdfs://nn1.hdp:
 *  2) 获取客户端对象
 *  3) 用户设置参数 配置对象
 *  put
 *  get
 *  ls
 *  mkdir -p
 *  rm -r
 *  cat
 *  echo 111 >>
 *  获取文件的元数据
 *  修改权限
 */
public class HdfsClientDemo1 {
    public static void main(String[] args) throws Exception {
        /**
         * HDP的默认配置对象
         *  1 自动读取默认配置文件
         *  2 用于用户自定义设置
         */
        Configuration conf = new Configuration();
        // 指定HDFS系统的位置
        URI uri = new URI("hdfs://nn1.hdp:8020");
        /**
         * url定位地址 uri定位资源,比如
         * jdbc:mysql://linux01:3306/demo 数据库
         * hdfs://nn1.hdp:8020/ 文件系统
         * jdbc:hive2://nn1.hdp:10000
         */
        //分布式文件系统
        FileSystem fs = FileSystem.newInstance(uri, conf, "root");

        //上传一个本地文件到HDFS系统的目录下
        /**
         * 参数1 path 本地文件路径
         * 参数2 path HDFS路径
         */

        fs.copyFromLocalFile(new Path("/Users/cosette/Desktop/bigdata/user.txt"), new Path("/"));

        fs.close();

    }

}
