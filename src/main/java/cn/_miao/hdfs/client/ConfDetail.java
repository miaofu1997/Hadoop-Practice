package cn._miao.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kerby.kerberos.kerb.type.pa.PaAuthenticationSet;

import java.net.URI;

/**
 * cn._miao.hdfs.client
 * @Auther: Cosette
 * @Date: 2021/2/3 16:42
 * @Description:
 *      1) 如果不设置 用默认参数
 *      2) 可以在代码中设置 conf.set(key, value)
 *      3) 也可以在resources目录下的hdfs-site.xml core-site.cml hdfs-default.xml文件中设置
 *      优先级:
 *          代码中 >> 项目配置文件中 >> 默认的参数
 *
 */
public class ConfDetail {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 改配置 也可以在resources里面写xml文件
//        conf.set("dfs.replication", "4");
//        conf.set("dfs.blocksize", "64M");
        FileSystem fs = FileSystem.newInstance(new URI("hdfs://nn1.hdp:8020"), conf, "root");

        fs.copyFromLocalFile(new Path("/Users/cosette/Software/MAC_SecureCRT_FX8.7.2.zip"), new Path("/MAC_SecureCRT_2.zip") );

        fs.close();
    }
}
