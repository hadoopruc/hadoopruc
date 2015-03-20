package buildindex;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
//Reduce函数，对于每个<key,(list of values)>pair调用一次reduce方法，对于每个Reduce
//为前面已经通过shuffle分好的，是按splitId来分的，因此每个split需要建立一个Index。
public class BuildIndexReduce extends Reducer<Text,Text,NullWritable,Writable>{
	    
		Index index = new Index();

		public void reduce(Text key,Iterable<Text> values,
				Context context)throws IOException,InterruptedException{
			index.init();//new Header,Footer,IndexContainer
			
			//index.indexcontainer.instance.clear();
			String str[] = key.toString().split("\\|");//切分key值

			long pos = 0;
			long recordnum = 0;
			//获取firstkey，此处默认数据是有序的，所以直接取第一个key值的属性值作为最小值，
			//假如数据此属性不有序不适用。
//			LongWritable firstkey = new LongWritable(Long.parseLong(str[1]));
			Text firstkey = new Text(str[1]);
			
			for(Text value:values){
				context.write(NullWritable.get(),value);//将原数据写到文件中
		
				str = key.toString().split("\\|");
				index.put(new Text(str[1]),new LongWritable(pos));
				pos += (value.getLength()+1);
				recordnum++;
			}
			//获取lastkey，此处默认数据是有序的，所以直接取最后一个key值的属性值作为最大值，
			//假如数据此属性不有序不适用。
//			LongWritable lastkey = new LongWritable(Long.parseLong(str[1]));
			Text lastkey = new Text(str[1]);
			//依次将indexcontainer,header,footer写入文件中
			index.setheader(new LongWritable(pos),firstkey,lastkey,new LongWritable(recordnum));
			context.write(NullWritable.get(),index.indexcontainer);
			long Mapsize = index.indexcontainer.size();
			index.setindexsize(new LongWritable(40+Mapsize));

			index.setfootersize(new LongWritable(16));
			index.setfooter(new LongWritable(pos+index.getindexsize().get()));
			context.write(NullWritable.get(),index.header);
			context.write(NullWritable.get(),index.footer);
		}
		
}
