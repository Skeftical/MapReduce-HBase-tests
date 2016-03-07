package task2;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by fotis on 04/03/16.
 */
public class HBaseMapper extends TableMapper<LongWritable, LongWritable> {
    private TimeZone tz = TimeZone.getTimeZone("UTC");
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private Date startDate;
    private Date endDate;
    public void setup (Context context){
        df.setTimeZone(tz);
        try{
            startDate = df.parse(context.getConfiguration().get("start"));
            endDate =  df.parse(context.getConfiguration().get("end"));
        }catch (Exception e){
            System.err.println("Unable to parse dates passed as arguments");
        }

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowkey=value.getRow();
        value.current().getTimestamp();
        long articleID = Bytes.toLong(Bytes.copy(rowkey, 0, 8));
        long revisionID = Bytes.toLong(Bytes.copy(rowkey, 8, 8));
        context.write(new LongWritable(articleID),new LongWritable(1));

    }
}
