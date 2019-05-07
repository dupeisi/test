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
 * ��������ʽ����hdfs�ļ�
 */
public class HdfsStreamDemo2 {

	public static void main(String[] args) throws Exception {
		//testDownload();
		testUpload();
	}

	@Test // �����ļ� �����ؽ���Ч��
	public static void testDownload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");
		Path srcPath = new Path("/dir1/aa.txt");

		// �ļ����ܳ���
		long totalLen = fs.getLength(srcPath);

		FSDataInputStream fsin = fs.open(srcPath);

		FileOutputStream fos = new FileOutputStream("e:/aa.txt");

		byte[] buffer = new byte[12];
		int len = 0;
		long currentLen = 0;// ��ǰ�������ļ�����

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

	@Test // �����ļ� �����ؽ���Ч��
	public static void testUpload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");

		// ��windows��d�̵�ĳ���ļ�
		FileInputStream fin = new FileInputStream("d:/java/jdk1.8/jdk-8u161-windows-x64.exe");

		FSDataOutputStream fsdos = fs.create(new Path("/java"));

		IOUtils.copy(fin, fsdos);

	}

	@Test // �����ļ� �����ؽ���Ч��
	public void testRandomDownload() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.44.4:9000"), conf, "root");

		FSDataInputStream fsin = fs.open(new Path("/dir1/aa.txt"));
		fsin.seek(30);// ��תָ��λ�� �Ӹ�λ�ÿ�ʼһֱ�����ļ�ĩβ

		FileOutputStream fos = new FileOutputStream("e:/rr.txt");

		IOUtils.copy(fsin, fos);
	}

}
