/**
 * 
 */
package cn.edu.bistu.mrpr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenruoyu
 *
 */
public class Test {
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		long danglingPr=1378000000000L;
//		double danglingPrAve=(double)danglingPr/PageRankJob.DANGLING_PR_FACTOR;
//		long pageCount = 15032;
//		System.out.println("页面总数:"+pageCount);
//		System.out.println("汇点PageRank累加值:"+danglingPr);
//		System.out.println("汇点PageRank累加值:"+danglingPrAve);
//		danglingPrAve=danglingPrAve/pageCount*PageRankJob.DELTA;
//		System.out.println("汇点PageRank均分值:"+danglingPrAve);
//		System.out.println(0.4*PageRankJob.DELTA+1-PageRankJob.DELTA+danglingPrAve);
//	}
	
	public static void main(String[] args){
//		String file = "/Users/chenruoyu/Downloads/hollins.dat";
//		String out = "/Users/chenruoyu/Downloads/pr.dat";
//		//处理hollins.dat中的内容，将其转换为PageRank算法所能接受的数据格式
//		Test test = new Test();
//		test.convert(file, out);
		String file = "/Users/chenruoyu/Downloads/part-m-00000";
		Test test = new Test();
		test.output(file);
	}
	
	public void output(String file){
		File input = new File(file);
		BufferedReader reader = null;
		List<PageRank> prs = new ArrayList<>();
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(input)) );
			String line = null;
			double total = 0;
			while((line = reader.readLine())!=null){
				String da[] = line.split(",");
				prs.add(new PageRank(Integer.parseInt(da[0]), Double.parseDouble(da[1])));
			}
			for(PageRank pr : prs){
				total+=pr.rank;
			}
			System.out.println("TOTAL RANK:"+total);
//			prs.sort(null);
//			for(int i=0;i<prs.size();i++){
//				PageRank pr = prs.get(i);
//				System.out.println("RANK:"+pr.rank+". PAGE:"+pr.page);
//			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void convert(String file, String out){
		//将file文件的内容转换为PageRank算法所能识别的数据格式,并写入out文件
		File input = new File(file);
		int no_of_pages = 0;
		int no_of_links = 0;
		Map<Integer,String> pages = new HashMap<Integer, String>();
		List<Link> links = new ArrayList();
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(input)) );
			//文件的第一行内容为两个数字，分别为页面和链接的个数，如：6012 23875
			String firstLine = reader.readLine();
			no_of_pages = Integer.parseInt(firstLine.substring(0, firstLine.indexOf(' ')));
			no_of_links = Integer.parseInt(firstLine.substring(firstLine.indexOf(' ')+1));
			//接下来的no_of_pages行是每个页面及其ID的对应关系，如：
			// 2 http://www.hollins.edu/
			for(int i=0;i<no_of_pages;i++){
				String page = reader.readLine();
				int id = Integer.parseInt(page.substring(0, page.indexOf(' ')));
				String url = page.substring(page.indexOf(' ')+1).trim();
				if(pages.containsKey(id)){
					System.out.println("出现重复页面ID！");
					System.out.println("已有："+id+" "+pages.get(id));
					System.out.println("新增："+id+" "+url);
				}else{
					pages.put(id, url);
				}
			}
			//再接下来的no_of_links行是链接信息，形式为：833 33
			//第一个数字为源页面ID，第二个数字为目标页面ID
			for(int i=0;i<no_of_links;i++){
				String page = reader.readLine();
				int src = Integer.parseInt(page.substring(0, page.indexOf(' ')));
				int dest = Integer.parseInt(page.substring(page.indexOf(' ')+1));
				links.add(new Link(src,dest));
			}
			links.sort(null);
			//读取完毕，可以生成邻接表形式的输出文件
			File output = new File(out);
			writer = new BufferedWriter(new FileWriter(output));
			int src = -1;
			StringBuffer dest = new StringBuffer();
			Set<Integer> srcPages = new HashSet<>();
			for(Link link:links){
				if(src==-1||link.src!=src){
					//新的src节点
					if(src!=-1){
						//之前的节点需要先写入文件
						writer.write(dest.toString());
						writer.newLine();
					}
					srcPages.add(link.src);
					src = link.src;
					dest = new StringBuffer();
					dest.append(src+",1,"+link.dest);
				}else{
					dest.append(",");
					dest.append(link.dest);
				}
			}
			//不要忘记最后一个页面
			//之前的节点需要先写入文件
			writer.write(dest.toString());
			writer.newLine();
			System.out.println(srcPages.size());
			//集中处理汇点页面
			for(int i=1;i<=no_of_pages;i++){
				if(!srcPages.contains(i)){
					writer.write(i+",1");
					writer.newLine();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try {
				reader.close();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	private class Link implements Comparable<Link>{
		public int src;
		public int dest;
		public Link(int src, int dest){
			this.src=src;
			this.dest=dest;
		}
		public int compareTo(Link o) {
			return Integer.compare(this.src, o.src);
		}
	}
	private class PageRank implements Comparable<PageRank>{
		public int page;
		public double rank;
		public PageRank(int page, double rank){
			this.page=page;
			this.rank=rank;
		}

		public int compareTo(PageRank o) {
			return -1*Double.compare(this.rank, o.rank);
		}
	}
	
}
