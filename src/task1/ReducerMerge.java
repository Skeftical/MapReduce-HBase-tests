package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fotis on 10/02/16.
 */
public class ReducerMerge extends Reducer<LongWritable, Text, LongWritable, Text> {
    static enum Counters{MORE_THAN_ONE};
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 1;
        while (values.iterator().hasNext()){
            context.write(key, values.iterator().next());
            if (counter == 2){
                context.getCounter(Counters.MORE_THAN_ONE).increment(1);
            }
            counter+=1;
        }
    }
}
