package cn.edu.bistu.mrpr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author twinsen
 */
public class PageRankMapper extends
		Mapper<LongWritable, Text, LongWritable, PRWritable> {
	private static final Log log = LogFactory.getLog(PageRankMapper.class);
	
	private LongWritable outKey = new LongWritable(1);//Mapper类的输出Key是页面的ID
	
	private PRWritable outValue = new PRWritable();//Mapper的输出Value有两种，一种是页面的邻接链表，一种是页面的当前部分PageRank值

	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		context.getCounter(PRCounters.PAGES_COUNT);
		context.getCounter(PRCounters.DANGLING_PAGE_PR);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			//Mapper的输入是页面的邻接链表，格式为：
			//srcID,pageRank,destID1,destID2,...
			//如：36,1,994,995,36,53 表示页面36的当前PageRank值为1，邻接链表为994，995，36，53
			//30,1 表示页面30的当前PageRank值为1，邻接链表为空
			String line = value.toString();
			String[] parts = line.split(",");
			if(parts.length<2){
				log.error("输入数据错误:"+value.toString());
				return;
			}
			/**
			 * 在输入文件中，每处理一行，即为一个页面，因此要对页面计数器加一
			 */
			context.getCounter(PRCounters.PAGES_COUNT).increment(1);
			long pageId = Long.parseLong(parts[0]);
			double pageRank = Double.parseDouble(parts[1]);
			int linkListSize = parts.length-2;
			if(linkListSize==0){
				//邻接链表为空,是汇点页面
				//首先，将源页面ID作为key，空的邻接链表作为value输出
				outKey.set(pageId);
				LLWritable out = new LLWritable(pageId, pageRank, new long[0]);
				outValue.set(out);
				context.write(outKey, outValue);
				//同时，将该页面的PageRank值处理后，累加到PRCounters.DANGLING_PAGE_PR这一Counter中，从而在Reducer中可以使用
				context.getCounter(PRCounters.DANGLING_PAGE_PR).increment((long)(pageRank*PageRankJob.DANGLING_PR_FACTOR));
			}else{
				//邻接链表不为空
				long[] ll = new long[linkListSize];
				//为每个目标页面设置PageRank
				double mass = pageRank/linkListSize;
				for(int i=2;i<parts.length;i++){
					outKey.set(Long.parseLong(parts[i]));
					outValue.set(new DoubleWritable(mass));
					context.write(outKey, outValue);
					ll[i-2]=Long.parseLong(parts[i]);
				}
				//将源页面ID作为key，邻接链表数组作为value输出
				outKey.set(pageId);
				LLWritable out = new LLWritable(pageId, pageRank, ll);
				outValue.set(out);
				context.write(outKey, outValue);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
