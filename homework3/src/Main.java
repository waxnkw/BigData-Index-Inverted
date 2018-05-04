
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import combine.SumCombiner;
import map.IndexInvertedMapper;
import partition.IndexInvertedPartioner;
import reduce.IndexInvertedReducer;

public class Main {

    public  static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //String[] otherArgs = new String[]{"/task2/*","/output"};

        if (otherArgs.length!=2){
            System.err.println("Usage: <in> <out> error");
            System.exit(2);
        }

        Job job = new Job(conf, "index-inverted");
        job.setJarByClass(Main.class);

        try{
            job.setMapperClass(IndexInvertedMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(IndexInvertedPartioner.class);
            job.setReducerClass(IndexInvertedReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }finally {

        }
    }
}
