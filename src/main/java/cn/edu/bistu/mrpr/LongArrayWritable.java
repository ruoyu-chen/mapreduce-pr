/**
 * 
 */
package cn.edu.bistu.mrpr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 对邻接链表的抽象，邻接链表本身是一个PageID的列表，PageID是以长整型表示的
 * @author chenruoyu
 *
 */
public class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
		super(LongWritable.class);
	}
	
	public LongArrayWritable(long[] longValues) {
		super(LongWritable.class);
		LongWritable[] values = new LongWritable[longValues.length];
		for (int i = 0; i < longValues.length; i++) {
			values[i] = new LongWritable(longValues[i]);
		}
		set(values);
	}
}
