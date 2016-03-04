package task2;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by fotis on 04/03/16.
 */
public class HBaseMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] title=value.getValue(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
        if (title!=null){
            context.write(new ImmutableBytesWritable(title),new IntWritable(1));
        }
    }
}
