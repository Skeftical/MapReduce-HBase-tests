package task3_f;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomPair, Text> {
	
		
	HashPartitioner<LongWritable, Text> hashPartitioner = new HashPartitioner<LongWritable, Text>();
	LongWritable newKey = new LongWritable();
	
	@Override
	public int getPartition(CustomPair key, Text val, int numRedTasks) {
		
		//return key.getArticleId().hashCode() % numRedTasks;
		newKey.set(key.getArticleId().get());
		
//		return newKey.hashCode() % numRedTasks;
		
		try {
			// Execute the default partitioner over the first part of the key
			newKey.set(key.getArticleId().get());
			return hashPartitioner.getPartition(newKey, val, numRedTasks);
		} catch (Exception e) {
			e.printStackTrace();
			// this would return a random value in the range 0 to number Of Reducers in case an exception is caught
			return (int) (Math.random() * numRedTasks);
		}
	}

}
