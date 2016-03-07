package task2.sol2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import task2.CustomPairMods;

import java.io.IOException;
import java.util.PriorityQueue;


/**
 * Created by fotis on 01/02/16.
 */
public class ReducerSol2 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    private PriorityQueue<CustomPairMods> pq = new PriorityQueue<CustomPairMods>();
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("k"));
    }

    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        int counter = 0; //More than one value ?
        long articleId = key.get();
        CustomPairMods customPairMods = null;

        if (values.iterator().hasNext()){
            counter+=1;
            int modifications = values.iterator().next().get();
            if (counter == 2){
                throw new IOException();
            }
            customPairMods = new CustomPairMods(articleId, modifications);
        }
        pq.add(customPairMods);
        if (pq.size() > k){
            pq.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        CustomPairMods customPairMods = null;
        while (pq.size() >0){
            customPairMods = pq.poll();
            context.write(new LongWritable(customPairMods.getArticleId()), new IntWritable(customPairMods.getModifications()));
        }
    }
}
