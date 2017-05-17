/**
 * 
 */
package cn.edu.bistu.mrpr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author chenruoyu
 *
 */
public class LLWritable implements Writable {

	private LongWritable pageId;

	/**
	 * 当前节点在前一轮的PageRank值，DoubleWritable类型
	 */
	private DoubleWritable prevPR;
	
	// 当前节点的邻接链表
	private LongArrayWritable linkList;

	public LLWritable() {
		this.pageId = new LongWritable(0);
		this.prevPR = new DoubleWritable(0);
		this.linkList = new LongArrayWritable();
	}

	public LLWritable(LongWritable pageId, DoubleWritable prevPR,
			LongArrayWritable linkList) {
		this.pageId = pageId;
		this.prevPR = prevPR;
		this.linkList = linkList;
	}

	public LLWritable(long pageId, double prevPR, long[] linkList) {
		this.pageId = new LongWritable(pageId);
		this.prevPR = new DoubleWritable(prevPR);
		this.linkList = new LongArrayWritable(linkList);
	}

	public LongWritable getPageId() {
		return pageId;
	}

	public void setPageId(LongWritable pageId) {
		this.pageId = pageId;
	}

	public DoubleWritable getPrevPR() {
		return prevPR;
	}

	public void setPrevPR(DoubleWritable prevPR) {
		this.prevPR = prevPR;
	}

	public LongArrayWritable getLinkList() {
		return linkList;
	}

	public void setLinkList(LongArrayWritable linkList) {
		this.linkList = linkList;
	}

	public void write(DataOutput dataOutput) throws IOException {
		pageId.write(dataOutput);
		prevPR.write(dataOutput);
		linkList.write(dataOutput);
	}

	public void readFields(DataInput dataInput) throws IOException {
		pageId.readFields(dataInput);
		prevPR.readFields(dataInput);
		linkList.readFields(dataInput);
	}

	@Override
	public int hashCode() {
		return pageId.hashCode();
	}
}
