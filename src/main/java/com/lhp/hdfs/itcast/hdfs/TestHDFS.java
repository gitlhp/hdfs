package com.lhp.hdfs.itcast.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class TestHDFS {
	 public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
		
		//获取hdfs客户端对象的时候，需要指定文件系统的类型为hdfs,同时也要注意用户的身份问题
		conf.set("fs.defaultFS", "hdfs://39.105.192.201:9000");

		//可以通过如下的方法进行客户端身份的伪造
		//System.setProperty("HADOOP_USER_NAME", "root");
		//首先需要一个hdfs的客户端对象
		 FileSystem fs = FileSystem.get(conf);
		 
		 RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);

		 //通过这个迭代器就可以遍历出我们hdfs文件系统根目录下的  文件

		 while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			Path path = fileStatus.getPath();
			String filename = path.getName();
			System.out.println(filename);

		}
		 
//		 fs.copyFromLocalFile(new Path("d://hello.txt"), new Path("/"));
//		 fs.close();
		 
//		 fs.copyToLocalFile(new Path("/hello.txt"), new Path("e://"));
////		 fs.copyToLocalFile(false, new Path("/hello.txt"), new Path("e://"), true);
//		 fs.close();
		 
	}
}
