package cn._miao.hdfs.client;

import cn._miao.hdfs.utils.MyUtils;
import org.apache.hadoop.fs.*;

/**
 * cn._miao.hdfs.client
 *
 * @Auther: Cosette
 * @Date: 2021/2/3 15:53
 * @Description:
 */
public class Ls {
    public static void main(String[] args) throws Exception {
//        ls();
//        isDir();

        return;
    }

    public static void isDir() throws Exception {
        FileSystem fs = MyUtils.getFs();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/data2"));
        for (int i = 0; i < fileStatuses.length; i++) {
            FileStatus fileStatus = fileStatuses[i];
            boolean b = fileStatus.isDirectory();
            if (b) { // 只打印文件夹
                Path path = fileStatus.getPath();
                String name = path.getName();
                System.out.println(path + "---" + name);
            }
        }
        fs.close();
    }

    public static void ls() throws Exception {
        FileSystem fs = MyUtils.getFs();
        // 所有文件
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/data2"), false);
        // 遍历每个文件
        while (locatedFileStatusRemoteIterator.hasNext()) {
            // 获取每个文件
            LocatedFileStatus file = locatedFileStatusRemoteIterator.next();
            // 获取文件路径
            Path path = file.getPath();
            // 获取文件名
            String name = path.getName();
            // 除此之外 还可以获取其他信息:
            long accessTime = file.getAccessTime();
            long len = file.getLen();
            long blockSize = file.getBlockSize(); // 文件逻辑切块大小 128M
            short replication = file.getReplication();

//            System.out.println(path + "---->" + name + "---->" + blockSize + "---->" + replication + "---->" + len);

            // 物理数据块真正的位置
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blocklocation: blockLocations) {
                long length = blocklocation.getLength(); //物理分块文件的实际大小
                String[] hosts = blocklocation.getHosts(); //副本所在所有的主机名字
                //每个副本所在主机名
                for (String host : hosts) {
                    System.out.println(name + "--" + host + "--" + length);
                }
            }
        }
        fs.close();
    }

}
