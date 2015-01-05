package query_without_index;


import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.util.LineReader;



public class StdQueryInputFormat extends FileInputFormat<LongWritable,Text>{
	private static final Log LOG = LogFactory.getLog(StdQueryInputFormat.class);
	 private static final double SPLIT_SLOP = 1.1;   // 10% slop
	 static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,TaskAttemptContext context) 
			throws IOException,InterruptedException{
		return new QRecordReader();
	}
	@Override
	protected boolean isSplitable(JobContext context,Path filename){
		return true;
	}

	public class QRecordReader extends RecordReader<LongWritable,Text>{
		private final Log LOG = LogFactory.getLog(QRecordReader.class);
		 public static final String MAX_LINE_LENGTH = 
				    "mapreduce.input.linerecordreader.line.maxlength";
		  private CompressionCodecFactory compressionCodecs = null;
		  private long start;
		  private long pos;
		  private long end;
		  private LineReader in;
		  private int maxLineLength;
		  private LongWritable key = null;
		  private Text value = null;
		  private Seekable filePosition;
		  private CompressionCodec codec;
		  private Decompressor decompressor;
		long lowkey = 900000;
		long highkey = 1000020;
		
		public void initialize(InputSplit genericSplit,
                TaskAttemptContext context) throws IOException {
			 FileSplit split = (FileSplit) genericSplit;
			    Configuration job = context.getConfiguration();
			    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
			                                    Integer.MAX_VALUE);
			    start = split.getStart();
			    end = start + split.getLength();
			    final Path file = split.getPath();
			    compressionCodecs = new CompressionCodecFactory(job);
			    codec = compressionCodecs.getCodec(file);

			    // open the file and seek to the start of the split
			    FileSystem fs = file.getFileSystem(job);
			    FSDataInputStream fileIn = fs.open(split.getPath());

			    if (isCompressedInput()) {
			      decompressor = CodecPool.getDecompressor(codec);
			      if (codec instanceof SplittableCompressionCodec) {
			        final SplitCompressionInputStream cIn =
			          ((SplittableCompressionCodec)codec).createInputStream(
			            fileIn, decompressor, start, end,
			            SplittableCompressionCodec.READ_MODE.BYBLOCK);
			        in = new LineReader(cIn, job);
			        start = cIn.getAdjustedStart();
			        end = cIn.getAdjustedEnd();
			        filePosition = cIn;
			      } else {
			        in = new LineReader(codec.createInputStream(fileIn, decompressor),
			            job);
			        filePosition = fileIn;
			      }
			    } else {
			      fileIn.seek(start);
			      in = new LineReader(fileIn, job);
			      filePosition = fileIn;
			    }
			    // If this is not the first split, we always throw away first record
			    // because we always (except the last split) read one extra line in
			    // next() method.
			    if (start != 0) {
			      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			    }
			    this.pos = start;
		}
		private boolean isCompressedInput() {
			    return (codec != null);
			  }
		private int maxBytesToConsume(long pos) {
			    return isCompressedInput()
			      ? Integer.MAX_VALUE
			      : (int) Math.min(Integer.MAX_VALUE, end - pos);
			  }
		 private long getFilePosition() throws IOException {
			    long retVal;
			    if (isCompressedInput() && null != filePosition) {
			      retVal = filePosition.getPos();
			    } else {
			      retVal = pos;
			    }
			    return retVal;
			  }
		public boolean nextKeyValue() throws IOException {
		      if (key == null) {
		          key = new LongWritable();
		      }
		    // key.set(Long.toString(split.getStart()));
		      // key.set(Long.toString(split.getStart())+Long.toString(pos));
		      if (value == null) {
			       value = new Text();
			     }
			  int newSize = 0;
		      while (getFilePosition() <= end) {
		        newSize = in.readLine(value, maxLineLength,
		                              Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
		                                       maxLineLength));
		    
		      if (newSize == 0) {
		         break;
		       }
		       pos += newSize;
		      if (newSize < maxLineLength) {
		         break;
		       }
			 
		      // line too long. try again
		      LOG.info("Skipped line of size " + newSize + " at pos " + 
		               (pos - newSize));
		     }


		      long currentkey = 0;
		    	  currentkey = ReadIndexedAttribute(value.toString());
		      if (newSize == 0) {
		       key = null;
		       value = null;
		        return false;
		      } else {		
		    	  if(currentkey < highkey&& currentkey>=lowkey){
		    	  key.set(pos);

		    	  return true;
		      }
		      else
		    	  return false;
		      }
		 }
		 @Override
		 public LongWritable getCurrentKey(){
			 return key;
		 }
		 @Override
		 public Text getCurrentValue(){
			 return value;
		 }
		 public float getProgress(){
			 if(start == end){
				 return 0.0f;
			 }else{
				 return Math.min(1.0f, (pos - start) / (float)(end - start));
			 }
		 }

		 
		 public long ReadIndexedAttribute(String s){
			 String str[] = s.split("\\|");
			 return Long.valueOf(str[0]);
		 }
		 public synchronized void close() throws IOException {
			     if (in != null) {
			       in.close(); 
		     }
		 }	
	}
}
