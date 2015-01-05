package buildindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public interface IndexContainer extends Writable {
	public void readFields(DataInput in) throws IOException;
	public void write(DataOutput out) throws IOException;
	public void put(LongWritable key,LongWritable value);
	public void remove(LongWritable key);
	public boolean containsKey(LongWritable key);
	public boolean containsValue(LongWritable value);
	public LongWritable get(LongWritable key);
	public long size();
}
