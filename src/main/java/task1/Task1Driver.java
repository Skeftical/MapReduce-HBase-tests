package task1;

import CustomInputFormat.MyInputFormat;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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
import task2.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by fotis on 31/01/16.
 */
public class Task1Driver extends Configured implements Tool {

    private static final String INTER_OUTPUT = "intermediate_output";

    public int run(String[] args) throws Exception{
        Job job1 = new Job(getConf());

        job1.setJobName("Task1");
        job1.setJarByClass(Task1Driver.class);


        if (args.length > 3){
            int numReducers = Integer.parseInt(args[3]);
            job1.setNumReduceTasks(numReducers);
        }


        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date startDate = null;
        Date endDate = null;

        df.setTimeZone(tz);
        try{
            startDate = df.parse(args[1]);
            endDate =  df.parse(args[2]);
        }catch (Exception e){
            System.out.println("Unable to parse dates passed as arguments");
            return 1;
        }


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

        Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
        scan.setCaching(1000);
        scan.setFilter(new KeyOnlyFilter());
        scan.setTimeRange(startDate.getTime(), endDate.getTime());
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("BD4Project2",scan, task1.HBaseMapper.class,
                CustomPair.class, LongWritable.class, job1);



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
        FileOutputFormat.setOutputPath(job2, new Path(args[0]));

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
        if (args.length < 3){
            System.out.println("Incorrect input");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.addResource(new Path("/users/level4/1200032s/bd4-hadoop/conf/core-site.xml"));
        conf.set("mapered.jar", "/users/level4/1200032s/bd4-hadoop/jar/BD_Assessed.jar");
        System.exit(ToolRunner.run(conf, new Task1Driver(), args));
    }
}

