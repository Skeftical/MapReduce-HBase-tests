package tutorial1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by fotis on 30/01/17.
 */
public class WordCountV1 extends Configured implements Tool {

    public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
                IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("WordCount-v1");
        job.setJarByClass(WordCountV1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args ) throws Exception{
        Configuration conf = new Configuration();
        conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
        System.exit(ToolRunner.run(conf, new WordCountV1(), args));
    }
}
