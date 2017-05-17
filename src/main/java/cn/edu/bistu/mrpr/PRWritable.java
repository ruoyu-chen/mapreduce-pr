/**
 * 
 */
package cn.edu.bistu.mrpr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * PageRank的并行算法的输出结果类
 * 
 * @author chenruoyu
 *
 */
@SuppressWarnings("unchecked")
public class PRWritable extends GenericWritable {
	private static Class<? extends Writable>[] CLASSES = null;
	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] {
				DoubleWritable.class, LLWritable.class };
	}

	public PRWritable() {
	}

	public PRWritable(Writable instance) {
		set(instance);
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}
}
