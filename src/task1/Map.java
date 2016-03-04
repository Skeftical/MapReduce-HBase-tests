package task1;

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
public class Map extends Mapper<LongWritable, Text, CustomPair, LongWritable> {
    private TimeZone tz = TimeZone.getTimeZone("UTC");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private Date startDate;
    private Date endDate;
    private CustomPair pair = new CustomPair();

    static enum Counters{MALFORMED_INPUT};
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
                    pair.setArticleId(Long.parseLong(array[1]));
                    pair.setRevisionId(Long.parseLong(array[2]));
                    context.write(pair,new LongWritable(pair.getRevisionId()));
                }
            }catch (ParseException e){
                context.getCounter(Counters.MALFORMED_INPUT).increment(1);
                e.printStackTrace();
            }catch (ArrayIndexOutOfBoundsException e){
                context.getCounter(Counters.MALFORMED_INPUT).increment(1);
                context.setStatus("Error array index out of bounds at line :"+value);
            }
        }
    }


}
