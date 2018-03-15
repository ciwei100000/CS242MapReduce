

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StopWordSkipper {
	
	public enum COUNTERS {
		  STOPWORDS,
		  GOODWORDS
		 }
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		
		Job job = Job.getInstance(conf, "StopWordSkipper");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(StopWordSkipper.class);
		job.setMapperClass(SkipMapReduce.SkipMapper.class);
		job.setReducerClass(SkipMapReduce.SkipReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		List<String> other_args = new ArrayList<>();
		// Logic to read the location of stop word file from the command line
		// The argument after -skip option will be taken as the location of stop
		// word file

		for (int i = 0; i < args.length; i++) {
			if ("-skip".equals(args[i])) {
				job.addCacheFile(new URI(args[++i]));
				if (i+1 < args.length)
				{
					i++;
				}
				else
				{
					break;
				}
			}
			
			other_args.add(args[i]);
		}

		FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		System.out.printf("Good Words: %d, Stop Words: %d\n",
			      counters.findCounter(COUNTERS.GOODWORDS).getValue(),
			      counters.findCounter(COUNTERS.STOPWORDS).getValue());
			 }
		}
	
