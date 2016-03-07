package task2.sol2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by fotis on 08/02/16.
 */
public class MapperSol2 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {


    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String[] array = value.toString().split("\\t");
        int articleId = Integer.parseInt(array[0]);
        int modifications =Integer.parseInt(array[1]);
        context.write(new LongWritable(articleId), new LongWritable(modifications));
    }


}
