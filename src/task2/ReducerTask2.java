package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by fotis on 01/02/16.
 */
public class ReducerTask2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

        int sum = 0;
        for (IntWritable val : values){
            sum+=val.get();
        }
        context.write(key, new IntWritable(sum));

    }


}
