package task1;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Created by fotis on 08/03/16.
 */
public class HBaseMapper extends TableMapper<CustomPair, LongWritable> {
    private CustomPair pair = new CustomPair();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowkey=value.getRow();
        long articleID = Bytes.toLong(Bytes.copy(rowkey, 0, 8));
        long revisionID = Bytes.toLong(Bytes.copy(rowkey, 8, 8));

        pair.setArticleId(articleID);
        pair.setRevisionId(revisionID);
        context.write(pair,new LongWritable(pair.getRevisionId()));
    }
}
