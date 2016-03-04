package task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondaryMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	static enum Counters {
		MAP_PROCESSED_JOB2
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.MAP_PROCESSED_JOB2).increment(1);
			String[] split = value.toString().split(" ");
			
			context.write(new LongWritable(Long.parseLong(split[0])), new Text(split[1]+ " " +split[2]));
			
	}
}
