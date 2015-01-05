package buildindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//自定义分组,compare函数为分组的比较函数，比较结果相等的item分为一组
public class BuildIndexGroupComparator extends WritableComparator {
	public BuildIndexGroupComparator(){
		super(Text.class,true);
	}
	@Override
	public int compare(WritableComparable a,WritableComparable b){
		String t1 = a.toString();
		String t2 = b.toString();
		String str1[] = t1.split("\\|");
		String str2[] = t2.split("\\|");
		return str1[0].compareTo(str2[0]);
		
	}
}
