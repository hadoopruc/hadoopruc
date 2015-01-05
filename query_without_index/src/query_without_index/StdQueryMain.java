package query_without_index;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class StdQueryMain {
	public static class StdQueryMapper extends
			Mapper<LongWritable, Text, LongWritable, Text>{
	    
	    private final static long lowkey = 1000000;
	    private final static long highkey = 1000010;
	      
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String [] str = value.toString().split("\\|");
	    	long id = Long.valueOf(str[0]);
	    	if(id >= lowkey && id < highkey)
	    	{
	    		context.write(key, value);
	      }
	    }
	  }
	public static class StdQueryReducer 
      extends Reducer<LongWritable,Text,LongWritable,Text> {

   public void reduce(LongWritable key, Iterable<Text> values, 
                      Context context
                      ) throws IOException, InterruptedException {

     for (Text val : values) {
         context.write(key, val);
     }

   }
 }
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: wordcount <in> <out>");
		  System.exit(2);
		}
		Job job = new Job(conf,"querymain");
		job.setJarByClass(StdQueryMain.class);
	//	FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
	//	job.setInputFormatClass(StdQueryInputFormat.class);
		job.setMapperClass(StdQueryMapper.class);
		job.setReducerClass(StdQueryReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(2);
	//	job.setPartitionerClass(BuildIndexShuffle.class);
	//	job.setSortComparatorClass(BuildIndexComparator.class);
	//	job.setGroupingComparatorClass(BuildIndexGroupComparator.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
		
	}

}
