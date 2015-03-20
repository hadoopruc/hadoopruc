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


public class BuildIndexMain
{
    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path inputPath = null;
        Path outputPath = null;
        int attributeNo = 0;
        if (otherArgs.length == 1)
        {
            inputPath = new Path(otherArgs[0]);
            String filename = inputPath.getName().split("\\.")[0];
            String directory = inputPath.getParent().toString();
            outputPath = new Path(directory + filename);
        } else if (otherArgs.length == 2)
        {
            inputPath = new Path(otherArgs[0]);
            String filename = inputPath.getName().split("\\.")[0];
            String directory = inputPath.getParent().toString();
            outputPath = new Path(directory + "/" + filename + "_index");
            attributeNo = Integer.parseInt(otherArgs[1]);
        } else
        {
            printUsage();
            System.exit(2);
        }
        //设置索引属性
        conf.setInt("attributeNo", attributeNo);

        Job job = new Job(conf, "buildindex");
        job.setJarByClass(BuildIndexInputFormat.class);
        FileSystem fs = FileSystem.get(conf);

        fs.delete(outputPath, true);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

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

    private static void printUsage()
    {
        System.out.println("-----------------------------------------------------------------");
        System.out.println("|Usage :   buildindex.sh [-a] <filepath> <AttributeNo> |");
        System.out.println("|index-built file path : <inputpath>/<filename>_index  |");
        System.out.println("|Default <AttrbuteNo> value is 0                       |");
        System.out.println("-----------------------------------------------------------------");
    }
}
