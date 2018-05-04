package reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class IndexInvertedReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static ArrayList<MyPair<String,Integer>> postList = new ArrayList();
    private static Text curTerm = new Text("");

    //context write part
    private static DoubleWritable termTimesAvg = new DoubleWritable();
    private static Text termOutput = new Text("");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values
            , Reducer<Text, IntWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String term = key.toString().split("#")[0];
        String doc = key.toString().split("#")[1];
        //sum of the term#doc
        int sum = 0;
        for (IntWritable val: values){
            sum += val.get();
        }

        //switch to next term
        //output last term
        if (curTerm!=null&&!term.equals(curTerm.toString())&&!curTerm.equals("")){
            writeResultOfTerm(context);
        }
        //update currentTerm
        curTerm = new Text(term);
        postList.add(new MyPair<>(doc, sum));
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        writeResultOfTerm(context);
        super.cleanup(context);
    }

    private void writeResultOfTerm(Reducer<Text, IntWritable, Text, Text>.Context context)throws IOException, InterruptedException{
        double avg = 0;
        StringBuilder out = new StringBuilder();
        for (MyPair<String,Integer> pair:postList){
            out.append(pair.getLeftVal()+":"+pair.getRightVal()+";");
            avg += pair.getRightVal();
        }
        avg/=(double)postList.size();
        termTimesAvg.set(avg);

        termOutput.set(termTimesAvg.toString()+","+out.toString());
        context.write(curTerm, termOutput);

        //initialize postList for new list
        postList = new ArrayList<>();
    }
}
