package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by fotis on 01/02/16.
 */
public class MapperTask2 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private TimeZone tz = TimeZone.getTimeZone("UTC");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private Date startDate;
    private Date endDate;
    static enum Counters {
        MAP_PROCESSED_JOB2
    }
    public void setup (Context context){
        df.setTimeZone(tz);
        try{
            startDate = df.parse(context.getConfiguration().get("start"));
            endDate =  df.parse(context.getConfiguration().get("end"));
        }catch (Exception e){
            System.err.println("Unable to parse dates passed as arguments");
        }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String val =value.toString();
        if (val.contains("REVISION")){
            String[] array = val.split(" ");
            try{
                Date date = df.parse(array[4]);
                if ((date.compareTo(startDate)== 1) && date.compareTo(endDate)== -1){
                    //Sending article id and revision id
                    context.getCounter(Counters.MAP_PROCESSED_JOB2).increment(1);
                    context.write(new IntWritable(Integer.parseInt(array[1])),new IntWritable(1));
                }
            }catch (ParseException e){
                e.printStackTrace();
            }catch (ArrayIndexOutOfBoundsException e){
                context.setStatus("Error array index out of bounds at line :"+value);
            }
        }
    }


}
