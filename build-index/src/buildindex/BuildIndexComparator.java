package buildindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//排序函数，对键值排序
public class BuildIndexComparator extends WritableComparator{
	public BuildIndexComparator(){
		super(Text.class,true);
	}
	//compare是实现的排序函数，此处为按照id属性排序
	@Override
	public int compare(WritableComparable a,WritableComparable b){
		String t1 = a.toString();
		String t2 = b.toString();
		String str1[] = t1.split("\\|");
		String str2[] = t2.split("\\|");
		long l1 = Long.parseLong(str1[1]);
		long l2 = Long.parseLong(str2[1]);
		return Long.compare(l1, l2);
	}

}
