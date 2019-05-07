package com.qf.hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * 
 * 以流的形式操作hdfs文件
 */
public class HdfsStreamDemo2 {

	public static void main(String[] args) throws Exception {
		//testDownload();
		testUpload();
	}

	@Test // 下载文件 带下载进度效果
	public static void testDownload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		Path srcPath = new Path("/dir1/aa.txt");

		// 文件的总长度
		long totalLen = fs.getLength(srcPath);

		FSDataInputStream fsin = fs.open(srcPath);

		FileOutputStream fos = new FileOutputStream("e:/aa.txt");

		byte[] buffer = new byte[12];
		int len = 0;
		long currentLen = 0;// 当前读到的文件长度

		while ((len = fsin.read(buffer)) != -1) {
			fos.write(buffer, 0, len);
			currentLen += len;
			float percent = currentLen * 1.0f / totalLen;
			System.out.println(percent);
			Thread.sleep(500);
		}

		fos.close();
		fsin.close();
	}

	@Test // 下载文件 带下载进度效果
	public static void testUpload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");

		// 读windows中d盘的某个文件
		FileInputStream fin = new FileInputStream("d:/java/jdk1.8/jdk-8u161-windows-x64.exe");

		FSDataOutputStream fsdos = fs.create(new Path("/java"));

		IOUtils.copy(fin, fsdos);

	}

	@Test // 下载文件 带下载进度效果
	public void testRandomDownload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");

		FSDataInputStream fsin = fs.open(new Path("/dir1/aa.txt"));
		fsin.seek(30);// 跳转指定位置 从该位置开始一直读到文件末尾

		FileOutputStream fos = new FileOutputStream("e:/rr.txt");

		IOUtils.copy(fsin, fos);
	}

}
