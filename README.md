# 使用MapReduce实现的PageRank算法

## 1.执行环境要求
软件依赖：Hadoop 2.7.3，JDK 1.8

执行环境配置：建议使用docker搭建执行环境。具体参见https://github.com/ruoyu-chen/hadoop-docker

## 2.测试数据格式

测试数据下载自http://www.limfinity.com/ir/data/hollins.dat.gz。

经过处理后，原始数据格式被转化为如下的邻接链表形式：

src, pr, dest<sub>1</sub>, dest<sub>2</sub>, ..., dest<sub>n</sub>

其中src为源页面id，pr为当前轮src页面的PageRank值，dest<sub>x</sub>为源页面所指向的目标页面id。

经过处理后的测试数据(pr.dat文件)可以从百度云下载：

链接: https://pan.baidu.com/s/1jI82WU6 密码: vaef

## 3.测试数据准备

安装配置好Hadoop运行环境后，需要将pr.dat文件上传到HDFS的/pr目录下，可以使用以下命令：

<pre><code>
# [创建/pr目录]
hadoop fs -mkdir /pr
# [上传测试文件]
hadoop fs -put /code/pr.dat /pr
</code></pre>

## 4.提交任务

运行下列代码，向集群提交任务：

<pre><code>hadoop jar MapReducePR.jar cn.edu.bistu.mrpr.PageRankJob /pr/ 
</code></pre>

程序执行完毕后，执行结果存放在HDFS文件系统中的/output目录下最新的一个目录中


## 5.参考资料

Data-Intensive Text Processing with MapReduce
https://lintool.github.io/MapReduceAlgorithms/index.html