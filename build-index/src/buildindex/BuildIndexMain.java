package buildindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class BuildIndexMain {
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: wordcount <in> <out>");
		  System.exit(2);
		}
		Job job = new Job(conf,"buildindex");
		job.setJarByClass(BuildIndexInputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		
		fs.delete(new Path("out"),true);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setInputFormatClass(BuildIndexInputFormat.class);
		job.setOutputFormatClass(BuildIndexOutputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(BuildIndexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setPartitionerClass(BuildIndexShuffle.class);
		job.setSortComparatorClass(BuildIndexComparator.class);
		job.setGroupingComparatorClass(BuildIndexGroupComparator.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
		
	}

}
