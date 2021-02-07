package cn._miao.hdfs.runjar;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * cn._miao.hdfs.runjar
 *
 * @Auther: Cosette
 * @Date: 2021/2/6 16:52
 * @Description:
 * reduce 单词hashCode取模 分到两个机器上
 */
public class HashCodeTest {
    public static void main(String[] args) throws Exception {
        String[] words = new String[]{"a", "a", "a", "b", "b", "b", "c", "c", "d"};
        FileOutputStream out1 = new FileOutputStream("/Users/cosette/Desktop/Bigdata/reduce1");
        FileOutputStream out2 = new FileOutputStream("/Users/cosette/Desktop/Bigdata/reduce2");

        for (String word : words) {
            int i = word.hashCode() % 2; // 0 或 1
            if (i==0) {
                out1.write(word.getBytes());
            } else {
                out2.write(word.getBytes());
            }
        }

        out1.close();
        out2.close();
    }
}
