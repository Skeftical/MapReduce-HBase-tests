package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by fotis on 01/02/16.
 */
public class ChainMapper2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
    private PriorityQueue<CustomPairMods> pq = new PriorityQueue<CustomPairMods>();
    private int k;

    public void setup (Context context){
        k = Integer.parseInt(context.getConfiguration().get("k"));
    }
    public void map(LongWritable key, LongWritable value, Context context) throws IOException,InterruptedException {
        long articleId = key.get();
        long modifications =value.get();
        CustomPairMods customPairMods = new CustomPairMods(articleId, modifications);
        pq.add(customPairMods);
        if (pq.size() > k){
            pq.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        CustomPairMods customPairMods;
        while(pq.size() > 0){
            customPairMods = pq.poll();
            context.write(new LongWritable(customPairMods.getArticleId()), new LongWritable(customPairMods.getModifications()));
        }
    }
}



