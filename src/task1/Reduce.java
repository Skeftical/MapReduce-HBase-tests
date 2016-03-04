package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by fotis on 01/02/16.
 */
public class Reduce extends Reducer<CustomPair, LongWritable, LongWritable, Text> {

    public void reduce(CustomPair key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {
        StringBuffer sb = new StringBuffer();
        int changes = 0;
        for (LongWritable val : values){
            sb.append(val.toString());
            sb.append(" ");
            changes+=1;
        }
        context.write(new LongWritable(key.getArticleId()), new Text(changes+" "+sb.toString()));

    }


}
