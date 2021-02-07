package cn._miao.hdfs.client;

import cn._miao.hdfs.utils.MyUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * cn._miao.hdfs.client
 *
 * @Auther: Cosette
 * @Date: 2021/2/2 20:30
 * @Description:
 * 文件的上传
 * 明确HDFS是 分布式文件系统 管理文件
 * 文件 --> 元数据 --> 位置
 * 最好不要直接操作多层的文件夹
 */
public class HdfsClientDemo2 {
    public static void main(String[] args) throws Exception {

        FileSystem fs = MyUtils.getFs();

        /**
         * 参数1 是否删除上传文件 默认false
         * 参数2 是否覆盖已存在文件 默认覆盖
         * 参数3 待上传的文件(可以是数组)
         * 参数4 HDFS目标路径
         */
        fs.copyFromLocalFile(true, true, new Path("/Users/cosette/Desktop/bigdata/user.txt"), new Path("/"));
        fs.close();
    }
}
