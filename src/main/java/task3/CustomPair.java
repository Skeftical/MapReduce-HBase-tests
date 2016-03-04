package task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CustomPair implements WritableComparable<CustomPair> {
	LongWritable article_id = new LongWritable();
	String date;

	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.article_id.set(in.readLong());
		this.date = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(article_id.get());
		WritableUtils.writeString(out, date);
	}

	@Override
	public int compareTo(CustomPair key) {
		
		if (key == null)
			return 0;
		
		
		int intArticle = this.article_id.compareTo(key.article_id);
		
		if (intArticle == 0){
			
			TimeZone tz = TimeZone.getTimeZone("UTC");
		    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	        df.setTimeZone(tz);

			int dateComp = 0;
			
			try {
				dateComp =  df.parse(this.getDate()).compareTo(df.parse(key.getDate()));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return -1 * dateComp;
		}
		else
			return intArticle;
	}
	
	
	public String getDate(){
		return this.date;
	}
	
	public void setDate(String date){
		this.date = date;
	}
	
	
	public LongWritable getArticleId(){
		return this.article_id;
	}
	
	public void setArticleId(Long article_id){
		this.article_id.set(article_id);
	}
}