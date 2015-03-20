package buildindex;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;
/** Partition keys by their {@link Object#hashCode()}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
//Shuffle主要是决定item分到哪个reducer上。
public class BuildIndexShuffle<K, V> extends Partitioner<K, V> {

	  /** Use {@link Object#hashCode()} to partition. */
	  public int getPartition(K key, V value,
	                          int numReduceTasks) {
		String str[] = key.toString().split("\\|");
		long i = Long.parseLong(str[0]);
	    return (int)(i%numReduceTasks);
	  }
}
