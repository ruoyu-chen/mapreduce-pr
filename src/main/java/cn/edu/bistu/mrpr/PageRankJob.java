package cn.edu.bistu.mrpr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class PageRankJob {
	private static final Log log = LogFactory.getLog(PageRankJob.class);

	public static final double DELTA = 0.85;// 摩擦系数、阻尼系数

	public static final int DANGLING_PR_FACTOR = 100000000;

	public static final double NON_CONVERGING_FACTOR = 0.01;

	public static final double DIFF = 0.001;

	public static void main(String args[]) throws IOException {
		if (args.length != 1) {
			log.error("缺少必要的输入文件参数");
			System.out.println("缺少必要的输入文件参数");
			System.exit(0);
		}
		long start = System.currentTimeMillis();
		long ncPageCount = 0;
		int iterationCount = 0;
		String inputFile = args[0];
		String outputFile = null;
		try {
			boolean needMoreRounds = true;
			do {
				log.info("第" + iterationCount + "轮迭代开始");
				Configuration conf = new Configuration();
				List<Path> inputFiles = FileUtil.getFiles(conf, inputFile);
				log.info("读取目录：" + inputFile + "中的文件共" + inputFiles.size() + "个做为输入");
				Job job = Job.getInstance(conf, "PageRank-Job-" + iterationCount);
				job.setJarByClass(PageRankJob.class);
				job.setInputFormatClass(TextInputFormat.class);
				for (Path input : inputFiles) {
					FileInputFormat.addInputPath(job, input);
				}
				job.setMapperClass(PageRankMapper.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(PRWritable.class);

				job.setReducerClass(PageRankReducer.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				outputFile = "/output/" + System.currentTimeMillis();
				FileOutputFormat.setOutputPath(job, new Path(outputFile));
				if (!job.waitForCompletion(true)) {
					System.err.println("PageRank Job Failed");
				}
				/**
				 * 第二阶段任务开始
				 */
				inputFiles = FileUtil.getFiles(conf, outputFile);
				log.info("读取目录：" + outputFile + "中的文件共" + inputFiles.size() + "个做为输入");
				Counters counters = job.getCounters();
				long danglingPagePr = counters.findCounter(PRCounters.DANGLING_PAGE_PR).getValue();
				long pageCount = counters.findCounter(PRCounters.PAGES_COUNT).getValue();
				conf = new Configuration();
				conf.set("DANGLING_PAGE_PR", String.valueOf(danglingPagePr));
				conf.set("PAGES_COUNT", String.valueOf(pageCount));
				Job prCompleteJob = Job.getInstance(conf, "PRComplete-Job-" + iterationCount);
				prCompleteJob.setJarByClass(PageRankJob.class);
				prCompleteJob.setInputFormatClass(TextInputFormat.class);
				for (Path input : inputFiles) {
					FileInputFormat.addInputPath(prCompleteJob, input);
				}
				prCompleteJob.setMapperClass(PRCompleteMapper.class);
				prCompleteJob.setMapOutputKeyClass(NullWritable.class);
				prCompleteJob.setMapOutputValueClass(Text.class);
				prCompleteJob.setNumReduceTasks(0);
				outputFile = "/output/" + System.currentTimeMillis();
				FileOutputFormat.setOutputPath(prCompleteJob, new Path(outputFile));
				if (!prCompleteJob.waitForCompletion(true)) {
					System.err.println("PRComplete Job Failed");
				}
				inputFile = outputFile;
				counters = prCompleteJob.getCounters();
				ncPageCount = counters.findCounter(PRCounters.NON_CONVERGING_PAGES).getValue();
				iterationCount++;
				if (ncPageCount < pageCount * NON_CONVERGING_FACTOR)
					needMoreRounds = false;
			} while (needMoreRounds);
			log.info("经过" + iterationCount + "轮迭代，计算结束");
			log.info("总用时:" + (System.currentTimeMillis() - start) + "ms");
			log.info("最终输出目录为："+outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}