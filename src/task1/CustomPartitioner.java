package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Created by fotis on 04/02/16.
 */
public class CustomPartitioner extends Partitioner<CustomPair,LongWritable> {

    public int getPartition(CustomPair customPair, LongWritable value, int numPartitions) {
        return customPair.hashCode() % numPartitions;
    }

}
