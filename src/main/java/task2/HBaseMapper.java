package task2;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
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
public class HBaseMapper extends TableMapper<LongWritable, IntWritable> {
    static enum Counters {
        MAP_PROCESSED_RECORDS
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counters.MAP_PROCESSED_RECORDS).increment(1);
        byte[] rowkey=value.getRow();
        long articleID = Bytes.toLong(Bytes.copy(rowkey, 0, 8));
        context.write(new LongWritable(articleID),new IntWritable(1));

    }
}
