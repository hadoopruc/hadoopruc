package buildindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
//Footer主要包含splitsize和footersize，其中splitsize是不包括Footer本身之外其他所有数据的大小
//footersize为Footer本身大小
public class Footer implements Writable{
	LongWritable splitsize = new LongWritable(0);
	LongWritable footersize = new LongWritable(16);
	public void set(LongWritable ssize){
		splitsize = ssize;
	}
	public void setfootersize(LongWritable fsize){
		footersize = fsize;
	}
	public LongWritable getspiltsize(){
		return splitsize;
	}
	public LongWritable getfootersize(){
		return footersize;
	}
	public String output(){
		String str = String.valueOf(splitsize)+"|"+String.valueOf(footersize);
		return str;
	}
	//计算Footer的大小
	public long size(){
		footersize = new LongWritable(16);
		return footersize.get();
	}
	//序列化写操作
	public void write(DataOutput out)throws IOException{
		splitsize.write(out);
		footersize.write(out);
	}
	//序列化读操作
	public void readFields(DataInput in)throws IOException{
		splitsize.readFields(in);
		footersize.readFields(in);
 
	}
}
