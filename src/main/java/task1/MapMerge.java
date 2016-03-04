package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by fotis on 10/02/16.
 */
public class MapMerge extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\\t");
        LongWritable keyout = new LongWritable(Long.parseLong(val[0]));
        Text newValue = new Text(val[1]);
        context.write(keyout, newValue);
    }
}
