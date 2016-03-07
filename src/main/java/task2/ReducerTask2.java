package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by fotis on 01/02/16.
 */
public class ReducerTask2 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {

        int sum = 0;
        for (LongWritable val : values){
            sum+=val.get();
        }
        context.write(key, new LongWritable(sum));

    }


}
