package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author zhangao
 * @version 2018.5.3
 * 倒排索引的map类
 * */
public class IndexInvertedMapper extends Mapper<Object, Text, Text, IntWritable> {

    //为了保证效率使用全局静态变量
    private static final IntWritable one = new IntWritable(1) ;
    private static Text word = new Text();

    /**
     * @param key
     * @param value
     * @param context 上下文
     * */
    @Override
    protected void map(Object key, Text value
            , Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException,InterruptedException {
        String fileName = getCurFileName(context);
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        //行内词语写入键值对
        for (;itr.hasMoreTokens();){
            String temp = itr.nextToken();
            word.set(temp+"#"+fileName);
            context.write(word, one);
        }
    }

    /**
     * 返回如 xxx.txt.segment中的书名部分
     * 比如 "金庸.txt.segment" 返回金庸
     * @param context 上下文
     * @return 书名
     * */
    private String getCurFileName(Context context){
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        int len = fileName.length();
        //System.out.println(fileName.substring(0,len-14));
        //.txt.segment长度为14
        return fileName.substring(0,len-14);
    }
}
