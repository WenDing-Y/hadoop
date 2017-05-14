package com.tech.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Read {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 FSDataInputStream in=null;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhostlei:8020");
			String filename="/storm/1.txt";
			FileSystem fSystem=FileSystem.get(conf);
			Path path=new Path(filename);
			in=fSystem.open(path);
			IOUtils.copyBytes(in, System.out,4096,false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
