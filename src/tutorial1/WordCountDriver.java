package tutorial1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by fotis on 30/01/17.
 */
public class WordCountDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("WordCount-v0");
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(TokenCounterMapper.class);
        job.setReducerClass(IntSumReducer.class);
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
        System.exit(ToolRunner.run(conf, new WordCountDriver(), args));
    }
}
