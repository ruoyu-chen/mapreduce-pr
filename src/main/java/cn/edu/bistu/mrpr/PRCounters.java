/**
 * 
 */
package cn.edu.bistu.mrpr;

/**
 * @author chenruoyu
 *
 */
public enum PRCounters {
	/**
	 * 总页面个数
	 */
	PAGES_COUNT,
	/**
	 * 汇点（无向外链接的页面）页面的PageRank值总和
	 */
	DANGLING_PAGE_PR,
	/**
	 * 还未收敛的页面的个数，换句话说，
	 * 就是在前后两轮迭代中，PR值之差超过阈值的页面的个数
	 */
	NON_CONVERGING_PAGES	
}
