package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Created by fotis on 04/03/16.
 */
public class ReducerAlt extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private PriorityQueue<CustomPairMods> pq = new PriorityQueue<CustomPairMods>();
    private int k;

    public void setup (Context context){
        k = Integer.parseInt(context.getConfiguration().get("k"));
    }
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

        int modifications = 0;
        for (IntWritable val : values){
            modifications+=val.get();
        }
        int articleId = key.get();
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
            context.write(new IntWritable(customPairMods.getArticleId()), new IntWritable(customPairMods.getModifications()));
        }
    }
}
