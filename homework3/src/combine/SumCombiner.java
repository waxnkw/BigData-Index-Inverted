package combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    //每个key 出现次数
    private static IntWritable times = new IntWritable();

    /**
     *相同的key,累加values
     * */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values
            , Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val: values){
            sum += val.get();
        }
        times.set(sum);
        context.write(key, times);
    }
}
