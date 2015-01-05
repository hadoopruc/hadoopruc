package query_with_index;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import buildindex.Footer;
import buildindex.Header;
import buildindex.Index;
import buildindex.Overlap;


public class QueryInputFormat extends FileInputFormat<LongWritable,Text>{
	private static final Log LOG = LogFactory.getLog(QueryInputFormat.class);
	 private static final double SPLIT_SLOP = 1.1;   // 10% slop
	 static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	 long FOOTER_SIZE = 16;
	 long HEADER_SIZE = 40;
	 int flag = 0;
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,TaskAttemptContext context) 
			throws IOException,InterruptedException{
		return new QRecordReader();
	}
	@Override
	protected boolean isSplitable(JobContext context,Path filename){
		return true;
	}
	@Override
	public List<InputSplit> getSplits(JobContext job
                                     ) throws IOException {
     long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
     long maxSize = getMaxSplitSize(job);
 
     // generate splits
     List<InputSplit> splits = new ArrayList<InputSplit>();
     List<FileStatus>files = listStatus(job);
     for (FileStatus file: files) {
       Path path = file.getPath();
       FileSystem fs = path.getFileSystem(job.getConfiguration());
       FSDataInputStream in = fs.open(path);

       long length = file.getLen();
       long offset = length;
       while(offset > 0)
       {
    //	byte [] buff = new byte[128];
		in.seek(offset-FOOTER_SIZE);
		Footer footer = new Footer();
		if(in != null)
		footer.readFields(in);
		//in.read(buff, 0, (int)FOOTER_SIZE);
		//String footerstr = new String(buff);
		//int i = footerstr.length();
		//String tmp = footerstr.substring(0, (int)(FOOTER_SIZE-1));//-1 means kicking the \n of this line out
		//String fstr [] = tmp.split("\\|");
		//Footer footer = new Footer();
		//footer.set(Long.parseLong(fstr[0]));
		//footer.setfootersize(Long.parseLong(fstr[1]));
		long splitsize = footer.getspiltsize().get();
		offset-= (splitsize+FOOTER_SIZE);
		 BlockLocation[] blkLocations = fs.getFileBlockLocations(file, offset, splitsize);
		 List <String> hostslist = new <String>ArrayList();
		 for(int i=0;i < blkLocations.length;i++)
			 for(int j=0 ;j <blkLocations[i].getHosts().length;j++)
				 hostslist.add(blkLocations[i].getHosts()[j]);
		 String [] splithosts = new String[hostslist.size()];
		 hostslist.toArray(splithosts);
		 splits.add(new FileSplit(path, offset, splitsize,
                 splithosts));
       }
    /*   BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
       if ((length != 0) && isSplitable(job, path)) {
         long blockSize = file.getBlockSize();
         long splitSize = computeSplitSize(blockSize, minSize, maxSize);
 
         long bytesRemaining = length;
         while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
           int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
           splits.add(new FileSplit(path, length-bytesRemaining, splitSize,
                                    blkLocations[blkIndex].getHosts()));
           bytesRemaining -= splitSize;
         }
 
         if (bytesRemaining != 0) {
           splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining,
                      blkLocations[blkLocations.length-1].getHosts()));
         }
       } else if (length != 0) {
         splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
       } else {
         //Create empty hosts array for zero length files
         splits.add(new FileSplit(path, 0, length, new String[0]));
       }*/
     }
 
     // Save the number of input files in the job-conf
     job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
 
     LOG.debug("Total # of splits: " + splits.size());
     LOG.info("Total # of splits: " + splits.size());
     return splits;
   }

	public class QRecordReader extends RecordReader<LongWritable,Text>{
		private final Log LOG = LogFactory.getLog(QRecordReader.class);
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;
		private LongWritable key = null;
		private Text value = null;
		private FileSplit split;
		long lowkey = 1000000;
		long highkey = 1000020;
		
		public void initialize(InputSplit genericSplit,
                TaskAttemptContext context) throws IOException {
			split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                          Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			fileIn.seek(end-HEADER_SIZE);
			

			Header h = new Header();
			h.readFields(fileIn);
			
			Overlap type = h.getOverlapType(new LongWritable(lowkey),new LongWritable(highkey));
			long offset;


			if(type == Overlap.LEFT_CONTAINED ||
				type == Overlap.FULL_CONTAINED ||
				 type == Overlap.POINT_CONTAINED){
					Index i = new Index();
					i.init();
					fileIn.seek(end-h.getindexsize().get());
					i.getMapindex().readFields(fileIn);
					i.setheader(h);
					offset = start + i.lookup(new LongWritable(lowkey)).get();
				}
			else if (type == Overlap.RIGHT_CONTAINED || 
					  type == Overlap.SPAN){
				offset = start;
			}
			else
				offset = end;
			fileIn.seek(offset);
			in = new LineReader(fileIn, job);	
			
		
			this.pos = offset;
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
		      while (pos < end) {
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
		      if (pos >= end) { 
			         return false;//there is the condition that Overlap is none and pos value is set to the end
			       }
		      flag++;
	//	      LOG.info(flag);
		      long currentkey = 0;
	//	      if(value.toString().length() != 0)
		    	  currentkey = ReadIndexedAttribute(value.toString());
		//      long currentkey = 0;
		      if (newSize == 0) {
		       key = null;
		       value = null;
		        return false;
		      } else {		
		    	  if(currentkey < highkey){
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
		/* public Header ReadHeader(FSDataInputStream in) throws IOException{
			 	byte [] buf= new byte[128];
				in.seek(end-HEADER_SIZE);
				in.read(buf, 0, (int)HEADER_SIZE);
				String headerstr = new String(buf);
				String tmp = headerstr.substring(0, (int)HEADER_SIZE-1);//-1 means kicking the "\n" of this line out.
				String hstr[] =  tmp.split("\\|");
				Header h = new Header();
				h.set(Long.parseLong(hstr[0]), Integer.parseInt(hstr[2]), 
						Integer.parseInt(hstr[3]), Long.parseLong(hstr[4]));
				h.setindexsize(Long.parseLong(hstr[1]));
				return h;
		 }*/
		/* public Index ReadIndex(FSDataInputStream in) throws IOException{
			 	Header h = ReadHeader(in);
			    byte [] buf= new byte[1024];
				in.seek(end-h.getindexsize());
				in.read(buf, 0, (int)h.getindexsize());
				String indexstr = new String(buf);
				String istr[] = indexstr.split("\n");
				String imaps[] = istr[0].split(",");
				int equalpos;
				Index index = new Index();
				index.init();
				index.setheader(h);
				for(int i = 0;i<imaps.length;i++)
				{
				    equalpos = 0;
					for(int k = 0;k < imaps[i].length();k++)
					if(imaps[i].charAt(k) == '='){
						equalpos=k;
						break;
					}

					String key = imaps[i].substring(1,equalpos);
					Long value;
					if(i == (imaps.length-1))
						value = Long.valueOf(imaps[i].substring(equalpos+1, imaps[i].length()-1));
					else
						value = Long.valueOf(imaps[i].substring(equalpos+1, imaps[i].length()));
					index.put(key, value);
				}
				return index;

		 }*/
		 
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
