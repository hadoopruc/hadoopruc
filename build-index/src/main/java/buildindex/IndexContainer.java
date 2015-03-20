package buildindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface IndexContainer extends Writable {
	public void readFields(DataInput in) throws IOException;
	public void write(DataOutput out) throws IOException;

	public void put(WritableComparable key, LongWritable value);

	public void remove(WritableComparable key);

	public boolean containsKey(WritableComparable key);
	public boolean containsValue(LongWritable value);

	public WritableComparable get(WritableComparable key);
	public long size();
}
