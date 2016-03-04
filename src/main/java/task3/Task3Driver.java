package task3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task3Driver extends Configured implements Tool {

	public static class Q3Map extends Mapper<LongWritable, Text, CustomPair, Text> {
		enum Counters {
			DATE_IN_EXCEPTIONS,EXCEPTIONS, MAP_OUT, DATE_NOT, VALUES_INPUT,TOTAL_LINES, ARTICLES_NOTUNIQUE
		}

		private TimeZone tz = TimeZone.getTimeZone("UTC");
		private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		private Date dateIn;
		
		private Long latestArticle_id = -1L;
		private Date latestDate;
		private String latestRevision_id = "";
		
		private Long article_id;
		private Date date1;
		private String revision_id;

		public void setup(Context context) {
			df.setTimeZone(tz);
			
			latestDate = new Date(Long.MIN_VALUE);
			
			
			try {
				dateIn = df.parse(context.getConfiguration().get("Date"));
			} catch (Exception e) {
				context.getCounter(Counters.DATE_IN_EXCEPTIONS).increment(1);
				System.err.println("Unable to parse dates passed as arguments");
			}

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val = value.toString();
			context.getCounter(Counters.TOTAL_LINES).increment(1);
			//split line
			String[] array = val.split(" ");
			
			if (val.contains("REVISION ") && array.length == 7) {
				context.getCounter(Counters.VALUES_INPUT).increment(1);
				

				// Parse Date
				try {
					date1 = df.parse(array[4]);
				} 
				catch (ParseException e) {
				context.getCounter(Counters.EXCEPTIONS).increment(1);
					e.printStackTrace();
				}
				
				if (!date1.after(dateIn)){
					
					// Parse Article_id
					article_id = Long.parseLong(array[1]);
					
					// Parse Revision_id
					revision_id = array[2];
					
					// If reached a new article_id
					if (latestArticle_id .compareTo(article_id) !=0){
						
						// If this is not the first run
						// Emit the Latest (article_id,date),revision_id
						if (latestArticle_id != -1L){
							CustomPair compKey = new CustomPair();
							compKey.setArticleId(latestArticle_id);
							compKey.setDate(df.format(latestDate));
							
							context.write(compKey, new Text(latestRevision_id.toString()));
						}
						
						// Set the latest article_id, revision_id, date to the new ones
						latestArticle_id = article_id;
						latestDate = date1;
						latestRevision_id = revision_id;
	
					}
					// If not a different article_id
					else{
						//context.getCounter(Counters.ARTICLES_NOTUNIQUE).increment(1);
	
						// Check if current date greater than the Latest Date
						if (latestDate.before(date1)){
							
							//Set the latest date, article_id, revision id to the entry with the lates date
							latestDate = date1;
							latestArticle_id = article_id;
							latestRevision_id = revision_id;
						}
						
					}
				}

			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			//Write the last one
			CustomPair compKey = new CustomPair();
			compKey.setArticleId(latestArticle_id);
			compKey.setDate(df.format(latestDate));
			
			context.write(compKey, new Text(latestRevision_id.toString()));
		}
	}

	public static class Q3Reduce extends Reducer<CustomPair, Text, LongWritable, Text> {

		public void reduce(CustomPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String revId = values.iterator().next().toString();

			context.write(new LongWritable(key.getArticleId().get()), new Text(revId + " " + key.getDate()));
			
			
		}
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		
		if (args.length <3){
			System.out.println("Invalid number of arguments.\nShould be in the form of <INPUT> <OUTPUT> <DATE> <NUM_OF_REDUCERS(optional)>");
			return 1;
		}

		Job job = new Job(getConf());
		job.setJobName("Q3");
		job.setJarByClass(Task3Driver.class);
		
		if (args.length==4)
			job.setNumReduceTasks(Integer.parseInt(args[3]));
		
		job.setMapperClass(Q3Map.class);
		job.setReducerClass(Q3Reduce.class);

		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(CustomGroupingComparator.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(CustomPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + new Path("Temporary")));

		job.getConfiguration().set("Date", args[2]);
		
		
		job.submit();
		boolean doneJob;
	
		job.waitForCompletion(true);
		
		//Second Job to merge files & sort everything
		Job secondary_job = new Job(getConf());
		secondary_job.setJobName("Q3_job_2");
		secondary_job.setJarByClass(Task3Driver.class);
		
		secondary_job.setNumReduceTasks(1);
			
			
		secondary_job.setInputFormatClass(TextInputFormat.class);
			
		secondary_job.setMapperClass(SecondaryMap.class);
		secondary_job.setReducerClass(SecondaryReduce.class);
						
		secondary_job.setOutputKeyClass(LongWritable.class);
		secondary_job.setOutputValueClass(Text.class);		
			
		secondary_job.setOutputFormatClass(TextOutputFormat.class);
			
		FileInputFormat.addInputPath(secondary_job, new Path(args[1] + new Path("Temporary")));
		FileOutputFormat.setOutputPath(secondary_job, new Path(args[1]));
		
		doneJob = secondary_job.waitForCompletion(true);

		
		if (doneJob){
			return 0;
		}
		else
			return 1;
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/users/level4/1200048c/Desktop/BD_Assessed/bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "/users/level4/1200048c/Desktop/BD_Assessed/myJar.jar");
        conf.set("mapreduce.output.textoutputformat.separator", " ");
		ToolRunner.run(conf, new Task3Driver(), args);
	}

}