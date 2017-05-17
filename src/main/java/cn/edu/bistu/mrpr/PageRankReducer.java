package cn.edu.bistu.mrpr;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author twinsen
 */
public class PageRankReducer extends Reducer<LongWritable, PRWritable, NullWritable, Text> {		
	//private static final Log log = LogFactory.getLog(PageRankReducer.class);
	private Text value = new Text();
	
	@Override
	protected void reduce(LongWritable key, Iterable<PRWritable> values, Context context)
			throws IOException, InterruptedException {
			long src = key.get();
			double sum = 0;//不考虑汇点和阻尼系数／摩擦系数所计算出来的部分PageRank值
			double prevPr = 0;//前一轮所计算出来的PageRank
			StringBuffer buf = new StringBuffer();
			for(PRWritable value:values){
				Writable v = value.get();
				if(v instanceof DoubleWritable){
					DoubleWritable dv = (DoubleWritable)v;
					sum+=dv.get();
				}else{
					LLWritable llwv = (LLWritable)v;
					prevPr = llwv.getPrevPR().get();//当前页面在前一轮时的PageRank值
					Writable[] longValues = llwv.getLinkList().get();//获取页面的邻接链表
					if(longValues!=null&&longValues.length>0){
						for(int i=0;i<longValues.length;i++){
							LongWritable destId = (LongWritable)longValues[i];
							buf.append(","+destId.get());
						}
					}
				}
			}
			value.set(src+","+prevPr+","+sum+buf.toString());
			context.write(null, value);
	}
}
