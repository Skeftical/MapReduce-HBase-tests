package task2;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by fotis on 04/03/16.
 */
public class HBaseMapper extends TableMapper<LongWritable, LongWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowkey=value.getRow();
        long articleID = Bytes.toLong(Bytes.copy(rowkey, 0, 8));
        long revisionID = Bytes.toLong(Bytes.copy(rowkey, 8, 8));
        context.write(new LongWritable(articleID),new LongWritable(revisionID));

    }
}
