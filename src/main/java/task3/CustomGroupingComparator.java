package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupingComparator extends WritableComparator {
	
	protected CustomGroupingComparator(){
		super(CustomPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	public int compare (WritableComparable w1, WritableComparable w2){
		CustomPair k1 = (CustomPair) w1;
		CustomPair k2 = (CustomPair) w2;
		
		//return Long.compare(k1.getArticleId().get(), k1.getArticleId().get());
		
		return k1.getArticleId().compareTo(k2.getArticleId());
	}

}
