package buildindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

//indexContainer实现，存储index的结构为java的Hashmap
//主要实现了序列化的读操作readFields(DataInput in)
//和序列化的写操作write(DataOutput out)
public class IndexContainerImpl implements IndexContainer
{
    public Map<WritableComparable, LongWritable> instance = new HashMap<WritableComparable, LongWritable>();

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.instance.clear();
        int entries = in.readInt();
        for (int i = 0; i < entries; i++)
        {
//            LongWritable key = new LongWritable();
//            key.readFields(in);
            WritableComparable key = new Text();
            key.readFields(in);
            LongWritable value = new LongWritable();
            value.readFields(in);
            instance.put(key, value);
        }

    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(instance.size());
        for (Map.Entry<WritableComparable, LongWritable> e : instance.entrySet())
        {

            e.getKey().write(out);

            e.getValue().write(out);
        }
    }

    public void put(WritableComparable key, LongWritable value)
    {

        instance.put(key, value);
    }

    public void remove(WritableComparable key)
    {
        instance.remove(key);
    }

    public boolean containsKey(WritableComparable key)
    {
        return instance.containsKey(key);
    }

    public boolean containsValue(LongWritable value)
    {
        return instance.containsValue(value);
    }

    public LongWritable get(WritableComparable key)
    {
        return instance.get(key);
    }

    //求IndexContainer的size()
    public long size()
    {
        return (instance.size() * 16 + 4);
    }
}
