package task3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> sortArray = new ArrayList<String>();
		
		while(values.iterator().hasNext()){
			sortArray.add(values.iterator().next().toString());
		}
		
		for(String val:sortArray){
			context.write(key, new Text(val));
		}
	}
}
