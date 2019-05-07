package com.qf.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class hadoop2 {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		fs.copyFromLocalFile(new Path("e:/aa.txt"), new Path("/wordcount/input"));
		fs.close();
	}
}
