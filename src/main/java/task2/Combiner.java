package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by fotis on 01/02/16.
 */
public class Combiner extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

        int sum = 0;
        for (IntWritable val : values){
            sum+=val.get();
        }
        context.write(key, new IntWritable(sum));

    }


}
