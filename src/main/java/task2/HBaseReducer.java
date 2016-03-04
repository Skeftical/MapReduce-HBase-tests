package task2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by fotis on 04/03/16.
 */
public class HBaseReducer extends Reducer<ImmutableBytesWritable, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable v : values){
            sum+=v.get();
        }
        context.write(new Text(new String(key.get())), new IntWritable(sum));

    }
}
