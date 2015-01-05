package buildindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
/*
 *Index包含Header,footer,indexcontainer 
 * 
 */
public class Index{
	Header header;
	Footer footer;
	IndexContainer indexcontainer;
	public void init(){
		header = new Header();
		footer = new Footer();
		indexcontainer = new IndexContainerImpl();
	}
	public void put(Text key,LongWritable value){
		long ikey = Long.parseLong(key.toString());
		LongWritable iwkey = new LongWritable(ikey);
	//	long lvalue = Long.parseLong(value);
		indexcontainer.put(iwkey, value);
	}
	public void remove(LongWritable key){
		indexcontainer.remove(key);
	}
	public boolean containkey(LongWritable key){
		return indexcontainer.containsKey(key);
	}
	public boolean containvalue(LongWritable value){
		return indexcontainer.containsValue(value);
	}
	//查询key所对应的value值，即pos值。
	public LongWritable lookup(LongWritable key){
		if(containkey(key) == true)
			return (LongWritable)indexcontainer.get(key);
		else
			return new LongWritable(0);
	}
	public int setheader(LongWritable dsize,LongWritable fkey,LongWritable lkey,LongWritable num){
		if(header == null)
			return 0;
		else
		{
			header.set(dsize, fkey, lkey, num);
			return 1;
		}
	}
	public int setheader(Header oheader){
		if(header == null)
			return 0;
		else
		{
			header.set(oheader.datasize, oheader.getfirstkey(),oheader.getlastkey(), oheader.getrecordnum());
			header.setindexsize(oheader.getindexsize());
			return 1;
		}
	}
	public int setfooter(LongWritable ssize){
		if(footer == null)
			return 0;
		else
		{
			footer.set(ssize);
			return 1;
		}

	}
	public boolean setindexsize(LongWritable isize){
		if(header == null)
			return false;
		else{
			header.setindexsize(isize);
			return true;
		}
	}
	public boolean setfootersize(LongWritable fsize){
		if(footer == null)
			return false;
		else{
			footer.setfootersize(fsize);
			return true;
		}
	}

	public LongWritable getindexsize(){
		return header.indexsize;
	}
	public LongWritable getfootersize(){
		return footer.footersize;
	}
	public LongWritable getsplitsize(){
		return footer.splitsize;
	}
	public IndexContainer getMapindex(){
		return indexcontainer;
	}
}
