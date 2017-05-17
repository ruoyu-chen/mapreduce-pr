package cn.edu.bistu.mrpr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 考虑了汇点以及阻尼系数等因素的PageRank算法第二阶段
 * @author twinsen
 */
public class PRCompleteMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	private static final Log log = LogFactory.getLog(PRCompleteMapper.class);
	
	private double danglingPrAve = 0;
	
	private Text outValue = new Text();
	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		context.getCounter(PRCounters.NON_CONVERGING_PAGES);
		Configuration conf = context.getConfiguration();
		long danglingPr = Long.parseLong(conf.get("DANGLING_PAGE_PR"));
		danglingPrAve=(double)danglingPr/PageRankJob.DANGLING_PR_FACTOR;
		long pageCount = Long.parseLong(conf.get("PAGES_COUNT"));
		danglingPrAve=danglingPrAve/pageCount*PageRankJob.DELTA;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			//Mapper的输入是页面的邻接链表，格式为：
			//srcID,prevPr,partialPr,destID1,destID2,...
			String line = value.toString();
			String[] parts = line.split(",");
			if(parts.length<3){
				log.error("输入数据错误:"+value.toString());
				return;
			}
			/**
			 * 在输入文件中，每处理一行，即为一个页面，因此要对页面计数器加一
			 */
			long pageId = Long.parseLong(parts[0]);
			double prevPr = Double.parseDouble(parts[1]);
			double partialPr = Double.parseDouble(parts[2]);
			double newPr = 0;
			int linkListSize = parts.length-3;
			/**
			 * 当前轮对PageRank的计算包括三部分：
			 * 由直接链接导入过来的PageRank，包含在partialPr中
			 * 固定的PageRank：1-DELTA，默认DELTA为0.85，因此这一部分值默认为0.15
			 * 由汇点贡献出来的PageRank，这一部分需要对汇点的PageRank值做累加，并除以系统中的页面总数，已经在danglingPrAve中计算好了
			 * 最终三部分将被带入下列公式，计算得出最终PageRank：
			 * Part1*DELTA+PART2+PART3
			 */
			newPr = partialPr*PageRankJob.DELTA + 1-PageRankJob.DELTA+danglingPrAve;
			StringBuffer buf = new StringBuffer();
			buf.append(pageId);
			buf.append(',');
			buf.append(newPr);
			if(linkListSize==0){
				//邻接链表为空,是汇点页面
				//将页面ID，页面PR，空的邻接链表作为value输出
				outValue.set(buf.toString());
				context.write(null, outValue);
			}else{
				//邻接链表不为空
				for(int i=3;i<parts.length;i++){
					buf.append(',');
					buf.append(Long.parseLong(parts[i]));
				}
				outValue.set(buf.toString());
				//将页面ID,PR,邻接链表作为value输出
				context.write(null, outValue);
			}
			double diff = 0;
			if(newPr>prevPr){
				diff=newPr-prevPr;
			}else{
				diff=prevPr-newPr;
			}
			if(diff>PageRankJob.DIFF){
				context.getCounter(PRCounters.NON_CONVERGING_PAGES).increment(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
