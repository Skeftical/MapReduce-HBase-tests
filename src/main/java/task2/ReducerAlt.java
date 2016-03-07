package task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Created by fotis on 04/03/16.
 */
public class ReducerAlt extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    private PriorityQueue<CustomPairMods> pq = new PriorityQueue<CustomPairMods>();
    private int k;

    @Override
    public void setup (Context context){
        k = Integer.parseInt(context.getConfiguration().get("k"));
    }

    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {

        long modifications = 0;
        for (LongWritable val : values){
            modifications+=val.get();
        }
        long articleId = key.get();
        CustomPairMods customPairMods = new CustomPairMods(articleId, modifications);
        pq.add(customPairMods);
        if (pq.size() > k){
            pq.poll();
        }

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        CustomPairMods customPairMods;
        while(pq.size() > 0){
            customPairMods = pq.poll();
            context.write(new LongWritable(customPairMods.getArticleId()), new LongWritable(customPairMods.getModifications()));
        }
    }
}
