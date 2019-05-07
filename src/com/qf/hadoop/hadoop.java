package com.qf.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class hadoop {
	public static void upload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		fs.copyFromLocalFile(new Path("e:/dd.txt"), new Path("/wordcount"));
		fs.close();
	}

	public static void download() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		fs.copyToLocalFile(new Path("/bb.txt"), new Path("e:/"));
		fs.close();
	}

	public static void main(String[] args) throws Exception {
		upload();
	}
}
