package com.qf.hadoop;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsStreamDemo {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		FSDataInputStream fsin = fs.open(new Path("/dir1/aa.txt"));

		FileOutputStream fos = new FileOutputStream("d:/bb.txt");
		byte[] arr = new byte[20];
		int len = 0;
		while ((len = fsin.read(arr)) != -1) {
			System.out.print(new String(arr, 0, len));
			// fos.write(arr, 0, len);
			// fos.close();
			// fsin.close();
		}

	}

}
