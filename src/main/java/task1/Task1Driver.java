package task1;

import CustomInputFormat.MyInputFormat;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by fotis on 31/01/16.
 */
public class Task1Driver extends Configured implements Tool {

    private static final String INTER_OUTPUT = "intermediate_output";

    public int run(String[] args) throws Exception{
        Job job1 = new Job(getConf());

        job1.setJobName("Task1");
        job1.setJarByClass(Task1Driver.class);


        if (args.length > 4){
            int numReducers = Integer.parseInt(args[4]);
            job1.setNumReduceTasks(numReducers);
        }

        job1.setMapperClass(Map.class);

        job1.setReducerClass(Reduce.class);
        job1.setGroupingComparatorClass(CustomGroupingComparator.class);
        job1.setPartitionerClass(CustomPartitioner.class);
        /*
        If SortComparator is left undefined RawComparator is used
        which is still the same thing however for reasons of clarity in our solution we are placing the sortComparator
        to explicitly show how the sorting is done.
         */
        job1.setSortComparatorClass(CustomSortComparator.class);


        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapOutputKeyClass(CustomPair.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);



        job1.getConfiguration().set("start",args[2]);
        job1.getConfiguration().set("end",args[3]);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(INTER_OUTPUT));


        Job job2 = new Job(getConf());
        job2.setJobName("Merge Records");
        job2.setJarByClass(Task1Driver.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapperClass(MapMerge.class);
        job2.setReducerClass(ReducerMerge.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(INTER_OUTPUT));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        int exitStatus = 0;
        List<Job> jobs = Lists.newArrayList(job1, job2);
        for (Job job : jobs){
            boolean jobSuccessful = job.waitForCompletion(true);
            if (!jobSuccessful){
                System.out.println("Error with job "+ job.getJobName()+" "+job.getStatus().getFailureInfo());
                exitStatus = 1;
            }
        }



        return exitStatus;

    }

    public static void main(String[] args) throws Exception{
        if (args.length < 4){
            System.out.println("Incorrect input");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.addResource(new Path("/users/level4/1200032s/bd4-hadoop/conf/core-site.xml"));
        conf.set("mapered.jar", "/users/level4/1200032s/bd4-hadoop/jar/BD_Assessed.jar");
        System.exit(ToolRunner.run(conf, new Task1Driver(), args));
    }
}

