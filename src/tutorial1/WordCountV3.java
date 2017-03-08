package tutorial1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by fotis on 31/01/17.
 */
public class WordCountV3 extends Configured implements Tool {

    private static class MyPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            int c = Character.toLowerCase(key.toString().charAt(0));
            if (c <'a' || c>'z')
                    return numPartitions - 1;
            return (int)Math.floor((float)(numPartitions - 2) * (c-'a')/('z'-'a'));
        }
    }

    private static class MyInputFormat extends FileInputFormat<LongWritable, Text> {
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordReader();
        }
    }

    private static class MyRecordReader extends RecordReader<LongWritable, Text> {
        private static final byte[] recordSeparator = "\t\t\t".getBytes();
        private FSDataInputStream fsin;
        private long start, end;
        private boolean stillInChunk = true;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) inputSplit;
            Configuration conf = context.getConfiguration();
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);
            fsin = fs.open(path);
            start = split.getStart();
            end = split.getStart() + split.getLength();
            fsin.seek(start);
            if (start !=0)
                readRecord(false);
        }

        private boolean readRecord(boolean withinBlock) throws IOException{
            int i=0,b;
            while (true) {
                if ((b =fsin.read()) == -1)
                    return false;
                if (withinBlock)
                        buffer.write(b);
                if(b==recordSeparator[i]) {
                    if (++i == recordSeparator.length)
                        return fsin.getPos() < end;
                }else {
                    i=0;
                }
            }
        }

        public boolean nextKeyValue() throws IOException {
            if (!stillInChunk)
                return false;
            boolean status = readRecord(true);
            value = new Text();
            value.set(buffer.getData(), 0, buffer.getLength());
            key.set(fsin.getPos());
            buffer.reset();
            if (!status)
                stillInChunk = false;
            return true;
        }

        public LongWritable getCurrentKey() {return key;}
        public Text getCurrentValue() {return value;}

        public float getProgress() throws IOException {
            return (float)(fsin.getPos() -start)/ (end-start);
        }

        public void close() throws IOException{fsin.close();}
    }


    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static enum Counters {NUM_RECORDS, NUM_LINES,NUM_BYTES}
        private Text _key = new Text();
        private IntWritable _value = new IntWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
            while (tokenizer.hasMoreTokens()){
                String line = tokenizer.nextToken();
                int sep = line.indexOf("");
                _key.set((sep==-1) ? line :line.substring(0, line.indexOf("")));
                _value.set(1);
                context.write(_key, _value);
                context.getCounter(Counters.NUM_LINES).increment(1);
            }

            context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
            context.getCounter(Counters.NUM_RECORDS).increment(1);
        }
    }

    public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable _value = new IntWritable();
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Iterator<IntWritable> it = values.iterator(); it.hasNext();)
                sum+=it.next().get();
            _value.set(sum);
            context.write(key, _value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job =new Job();
        job.setJobName("WordCountV3");
        job.setJarByClass(WordCountV3.class);
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args ) throws Exception{
        Configuration conf = new Configuration();
        conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
        System.exit(ToolRunner.run(conf, new WordCountV3(), args));
    }
}

