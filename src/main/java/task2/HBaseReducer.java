package task2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fotis on 04/03/16.
 */
public class HBaseReducer extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (LongWritable v : values){
            sum+=v.get();
        }
        context.write(key, new IntWritable(sum));

    }
}
