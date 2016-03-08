package task2;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import task2.sol2.MapperSol2;
import task2.sol2.ReducerSol2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


public class Task2Driver extends Configured implements Tool {


    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());


        if (args.length > 4){
            int numReducers = Integer.parseInt(args[4]);
            job.setNumReduceTasks(numReducers);
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



        job.setJobName("Task2");
        job.setJarByClass(Task2Driver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(ReducerAlt.class);



        Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
        scan.setCaching(1000);
        scan.setFilter(new KeyOnlyFilter());
        scan.setTimeRange(startDate.getTime(), endDate.getTime());
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("BD4Project2Sample",scan, HBaseMapper.class,
                LongWritable.class, IntWritable.class, job);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

//        Job job2 = new Job(getConf());
//
//        job2.setJarByClass(Task2Driver.class);
//        job2.setJobName("TOPK");
//        job2.setInputFormatClass(TextInputFormat.class);
//        job2.setOutputFormatClass(TextOutputFormat.class);
//        job2.setNumReduceTasks(1);
//        job2.setMapperClass(MapperSol2.class);
//        job2.setReducerClass(ReducerSol2.class);
//        job2.setMapOutputKeyClass(LongWritable.class);
//        job2.setMapOutputValueClass(IntWritable.class);
//
//        job2.setOutputKeyClass(LongWritable.class);
//        job2.setOutputValueClass(IntWritable.class);
//
//        TextInputFormat.addInputPath(job2, new Path(INTER_OUTPUT));
//        TextOutputFormat.setOutputPath(job2, new Path(args[1]));


        String k = args[3];
        job.getConfiguration().set("k",k);
//        job2.getConfiguration().set("k",k);

        List<Job> jobs = Lists.newArrayList(job);
        int exitStatus = 0;
        for (Job vjob : jobs){
            boolean jobSuccessful = vjob.waitForCompletion(true);
            if (!jobSuccessful){
                System.out.println("Error with job "+ vjob.getJobName()+" "+vjob.getStatus().getFailureInfo());
                exitStatus = 1;
                break;
            }
        }
        return exitStatus;
    }

    public static void main(String[] args) throws Exception{
        if (args.length < 4){
            System.out.println("Incorrect input");
            System.exit(0);
        }
        Configuration conf = new HBaseConfiguration().create();
        conf.addResource(new Path("/users/level4/1200032s/bd4-hadoop/conf/core-site.xml"));
        conf.set("mapered.jar", "/users/level4/1200032s/bd4-hadoop/jar/main.jar");
        System.exit(ToolRunner.run(conf, new Task2Driver(), args));
    }
}

