package tutorial1;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by fotis on 30/01/17.
 */
public class WordCountV2 extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        static enum COUNTERS{INPUT_WORDS};
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = true;
        private Set<String> patternsToSkip = new HashSet<String>();
        private long numRecords = 0;
        private String inputFile;

        private void parseSkipFile(Path patternsFile){
            try {
                BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
                String pattern = null;
                while ((pattern = fis.readLine()) !=null){
                    patternsToSkip.add(pattern);
                }
                fis.close();
            }catch (IOException ioe){
                System.err.println("Exception");
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            inputFile = conf.get("map.input.file");

            if (conf.getBoolean("wordcount.skip.patterns", false)){
                Path[] patternsFiles = new Path[0];
                try{
                    patternsFiles = DistributedCache.getLocalCacheFiles(conf);
                } catch (IOException ioe){
                    System.err.println("Exception cached files");
                }
                for (Path patternsFile : patternsFiles)
                    parseSkipFile(patternsFile);

            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = caseSensitive ? value.toString() : value.toString().toLowerCase();

            for (String pattern : patternsToSkip)
                line = line.replaceAll(pattern, "");

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                context.write(word, one);
                context.getCounter(COUNTERS.INPUT_WORDS).increment(1);
            }

            if ((++numRecords % 100) == 0)
                context.setStatus("Finished processsing "+numRecords +" records from input file" + inputFile);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            patternsToSkip.clear();
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
        job.setJobName("WordCount-v2");
        job.setJarByClass(WordCountV2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i<args.length; ++i){
            if ("-skip".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job.getConfiguration());
            }else {
                otherArgs.add(args[i]);
            }
        }


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args ) throws Exception{
        Configuration conf = new Configuration();
        conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
        System.exit(ToolRunner.run(conf, new WordCountV2(), args));
    }
}
