package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class IndexInvertedPartioner extends HashPartitioner<Text, IntWritable> {
    private static Text word = new Text();

    /**
     * 提取term#doc中term作为partition的key
     * @param key term#doc形式
     * @param value
     * @param numReduceTasks reducer node num
     * @return
     * */
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String term = key.toString().split("#")[0];
        word.set(term);
        return super.getPartition(word, value, numReduceTasks);
    }
}
