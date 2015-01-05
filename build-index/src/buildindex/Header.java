package buildindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/*
 * Header包含datasize,indexsize,firstkey,lastkey,Recordnum
 * datasize:原始数据的大小
 * firstkey:建索引属性值的最小值
 * lastkey:建索引属性值的最大值
 * Recordnum：record的数量
*/
public class Header implements Writable{
	LongWritable datasize = new LongWritable(0);
	LongWritable indexsize = new LongWritable(0);   
	LongWritable firstkey = new LongWritable(0);
	LongWritable lastkey = new LongWritable(0);
	LongWritable Recordnum = new LongWritable(0);
	public void set(LongWritable dsize,LongWritable fkey,LongWritable lkey,LongWritable num){
		datasize = dsize;
		firstkey = fkey;
		lastkey = lkey;
		Recordnum = num;

	}
	public void setindexsize(LongWritable isize)
	{
		indexsize = isize;
	}
	public void setindexpos(LongWritable pos)
	{
		indexsize = pos;
	}
	public LongWritable getdatasize(){
		return datasize;
	}
	public LongWritable getindexsize(){
		return indexsize;
	}
	public LongWritable getfirstkey(){
		return firstkey;
	}
	public LongWritable getlastkey(){
		return lastkey;
	}
	public LongWritable getrecordnum(){
		return Recordnum;
	}
	//计算lowkey和highkey区间和split的区间的重叠关系，从而通过此决定偏移量移动到哪里
	public Overlap getOverlapType(LongWritable lowkey,LongWritable highkey){
		if(highkey.get()<firstkey.get() || lowkey.get()>lastkey.get())
			return Overlap.NOT_CONTAINED;
		else if(lowkey.get()<firstkey.get() && highkey.get()<lastkey.get())
			return Overlap.RIGHT_CONTAINED;
		else if(lowkey.get()<firstkey.get() && highkey.get()>=lastkey.get())
			return Overlap.SPAN;
		else if(highkey.get()>lastkey.get() && lowkey.get()>=firstkey.get())
			return Overlap.LEFT_CONTAINED;
		else if(highkey.get()<=lastkey.get() && lowkey.get()>=firstkey.get())
			return Overlap.FULL_CONTAINED;
		else if(lowkey == lastkey)
			return Overlap.POINT_CONTAINED;
		else
			return null;
	}
	//计算Header的大小
	public long size(){
		indexsize = new LongWritable(40);
		return indexsize.get();
	}
	public String output(){
		String str = String.valueOf(datasize)+"|"+String.valueOf(indexsize)+"|"+
						String.valueOf(firstkey)+"|"+String.valueOf(lastkey)+"|"+String.valueOf(Recordnum);
		return str;
	}
	//序列化写操作
	public void write(DataOutput out)throws IOException{
		datasize.write(out);
		indexsize.write(out);
		firstkey.write(out);
		lastkey.write(out);
		Recordnum.write(out);

	}
	//序列化读操作
	public void readFields(DataInput in)throws IOException{
		datasize.readFields(in);
		indexsize.readFields(in);
		firstkey.readFields(in);
		lastkey.readFields(in);
		Recordnum.readFields(in);

	}

	
	
}
