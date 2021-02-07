package cn._miao.hdfs.client;

import cn._miao.hdfs.utils.MyUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * cn._miao.hdfs.client
 *
 * @Auther: Cosette
 * @Date: 2021/2/2 21:21
 * @Description:
 */
public class ClientTest {
    public static void main(String[] args) throws Exception {
//        deleteDirOrFile();
//        mkdirs();
//        rename();
//        moveAndRename();
//        readData();
//        writeOverwrite();
//        append();


        return;
    }

    /**
     * 追加写
     * @throws Exception
     */
    private static void append() throws Exception {
        FileSystem fs = MyUtils.getFs();
        Path path = new Path("/data2/user.log");
        //追加写, 文件必须存在, 否则会报错
        if(fs.exists(path)) {
            FSDataOutputStream append = fs.append(path);
            append.write("\n11111\n".getBytes());
            append.write("1111111111\n".getBytes());
            append.flush();
            append.close();
        }
        fs.close();
    }

    /**
     * 覆盖写
     * @throws Exception
     */
    private static void writeOverwrite() throws Exception {
        FileSystem fs = MyUtils.getFs();
        Path path = new Path("/data2/user2.log");
        //覆盖写, 文件可以存在也可以不存在
        FSDataOutputStream fout = fs.create(path);

        fout.write("hello boys".getBytes());
        fout.flush();
        fout.close();

        fs.close();
    }

    /**
     * 读数据
     * @throws Exception
     */
    private static void readData() throws Exception {
        FileSystem fs = MyUtils.getFs();
        Path path = new Path("/data2/user.log");
        FSDataInputStream fsins = fs.open(path);

        BufferedReader br = new BufferedReader(new InputStreamReader(fsins));
        String line = null;
        while ((line=br.readLine())!=null) {
            System.out.println(line);
        }
        br.close();
        fs.close();
    }

    /**
     * 移动+重命名
     * @throws Exception
     */
    private static void moveAndRename() throws Exception {
        FileSystem fs = MyUtils.getFs();
        fs.rename(new Path("/hadoop-3.1.1.tar.gz"), new Path("/data2/a.tar.gz"));
        fs.close();
    }

    /**
     * 重命名
     * @throws Exception
     */
    private static void rename() throws Exception {
        FileSystem fs = MyUtils.getFs();
        fs.rename(new Path("/user.txt"), new Path("/user.log"));
        fs.close();
    }

    /**
     * 创建文件夹
     * @throws Exception
     */
    private static void mkdirs() throws Exception {
        FileSystem fs = MyUtils.getFs();
        fs.mkdirs(new Path("/test/a/b")) ;
        fs.close();
    }

    /**
     * 删除
     * @throws Exception
     */
    public static void deleteDirOrFile() throws Exception {
        FileSystem fs = MyUtils.getFs();
        Path path = new Path("/data");
        boolean exists = fs.exists(path);
        if(exists){
            /**
             * 参数1 要删除的路径
             * 参数2 强制递归
             */
            fs.delete(path, true);
        } else {
            System.out.println("删除路径不存在!");
        }
        fs.close();
    }
}
