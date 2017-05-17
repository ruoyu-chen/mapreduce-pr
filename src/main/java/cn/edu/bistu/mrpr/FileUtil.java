/**
 * 
 */
package cn.edu.bistu.mrpr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author hadoop
 *
 */
public class FileUtil {
	
	/**
	 * 获取HDFS上给定的目录（path）下所有大小不为0的文件的列表并返回。
	 * @param conf
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<Path> getFiles(Configuration conf, String path) throws FileNotFoundException, IOException{
		List<Path> result = new ArrayList<>();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(path));
			for(FileStatus file : status){
				//只处理path目录下的文件，不进行递归处理
				if(!file.isDirectory()){
					if(file.getLen()!=0){
						result.add(file.getPath());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
