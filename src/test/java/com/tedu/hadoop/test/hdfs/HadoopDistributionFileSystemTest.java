package com.tedu.hadoop.test.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Random;

public class HadoopDistributionFileSystemTest {

	static final String FS = "hdfs://h201.c8:8020";
	static final Configuration CONF = new Configuration();

	static {
		CONF.set("fs.defaultFS", FS);
		// This method can be called at most once in a given JVM.
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	@Test
	public void testGetSystem() throws Exception {
		FileSystem system = FileSystem.get(CONF);
		// System.out.println(system.toString());
		system.close();
	}

	@Test
	public void testCopyToLocalFile() throws Exception {
		System.out.println("== 下载操作 ==");
		File file = new File("E:\\a1.txt");
		FileSystem system = FileSystem.get(new URI(FS), CONF);
		System.out.println("by system.copyToLocalFile");
		system.copyToLocalFile(new Path("/dir1/a"), new Path(file.getAbsolutePath()));
		if (file.exists()) {
			System.out.printf("/dir1/a 下载到 %s 成功！\n", file.getAbsoluteFile());
			file.deleteOnExit();
		} else {
			System.out.printf("/dir1/a 下载到 %s 失败！\n", file.getAbsoluteFile());
		}
	}

	@Test
	public void testCopyBytes() throws IOException {
		System.out.println("== 下载操作 ==");
		File file = new File("/a2.txt");
		InputStream in = new URL(FS.concat("/dir1/a.txt")).openStream();
		FileOutputStream out = new FileOutputStream(file.getAbsoluteFile());
		System.out.println("by IOUtils.copyBytes");
		IOUtils.copyBytes(in, out, 1024, true);

		if (file.exists()) {
			System.out.printf("/dir1/a 下载到 %s 成功！\n", file.getAbsoluteFile());
			file.deleteOnExit();
		} else {
			System.out.printf("/dir1/a 下载到 %s 失败！\n", file.getAbsoluteFile());
		}
	}


	@Test
	public void testListFiles() throws Exception {
		System.out.println("== 列举文件列表 ==");
		// FileSystem system = FileSystem.newInstance(new URI(FS), CONF);
		FileSystem system = FileSystem.get(CONF);

		RemoteIterator<LocatedFileStatus> files = system.listFiles(new Path("/dir1"), true);
		while (files.hasNext()) {
			LocatedFileStatus status = files.next();
			System.out.println(status.getPath());
		}
		system.close();
	}

	@Test
	public void testMkdirs() throws Exception {
		System.out.println("== 创建目录 ==");
		Path dest = new Path("/usr/local");
		FileSystem system = FileSystem.get(new URI(FS), CONF, "root");
		boolean exists = system.mkdirs(dest);
		System.out.printf("创建 %s%s %s\n", FS, dest.toUri(), (exists ? "成功" : "失败"));
	}

	@Test
	public void testOperation() throws Exception {
		final Path dest = new Path("/usr/local/hosts");
		final String src = "C:\\Windows\\System32\\drivers\\etc\\hosts";
		final boolean useCopyBytes = Math.random() > 0.5;
		System.out.println("== 上传操作 ==");
		{
			FileSystem system = FileSystem.get(new URI(FS), CONF, "root");
			if (useCopyBytes) {
				System.out.println("by IOUtils.copyBytes");
				OutputStream out = system.create(dest);
				FileInputStream in = new FileInputStream(src);
				IOUtils.copyBytes(in, out, 1024);
			} else {
				System.out.println("by system.copyFromLocalFile");
				system.copyFromLocalFile(new Path(src), dest);
			}
			system.close();
		}
		// System.out.println("== exists ==");
		{
			FileSystem system = FileSystem.get(new URI(FS), CONF, "root");
			String exists = system.exists(dest) ? "成功" : "失败";
			System.out.printf("%s 上传到 %s%s %s\n", src, FS, dest.toUri(), exists);
			system.close();
		}
		System.out.println("== 删除操作 ==");
		{
			FileSystem system = FileSystem.get(new URI(FS), CONF, "root");
			boolean deleted = system.delete(new Path("/usr/local/hosts"), false);
			System.out.println("删除 /usr/local/hosts " + (deleted ? "成功" : "失败"));
			system.close();
		}
	}

	private File randomFileGenerate() throws IOException {
		File dir = new File("/files");
		Random random = new Random();
		int total = 50 + random.nextInt(150);
		for (int i = 0; i < total; i++) {
			// 0 代表前面补充 0
			// 3 代表长度为 3
			// d 代表参数为正数型
			String num = String.format("%03d", i);
			File src = new File(dir, "random-" + num);
			String data = RandomStringUtils.randomAlphanumeric(5 + random.nextInt(100));
			data = String.format("%s.\t%s\r\n", num, data);
			System.out.printf("%s\t%s\n", num, src.getAbsoluteFile());
			FileUtils.writeStringToFile(src, data);
		}
		return dir;
	}

	@Test
	public void genLocal() throws IOException {
		File dir = randomFileGenerate();
		File outfile = new File("F:/Repository/Git/git-local-demo/gen.txt");
		OutputStream out = new FileOutputStream(outfile);
		File[] files = dir.listFiles();
		if (files != null) {
			for (File file : files) {
				InputStream in = new FileInputStream(file);
				IOUtils.copyBytes(in, out, 1024);
				out.flush();
				in.close();
			}
		}
		out.close();
		// 删除文件集合
		FileUtils.deleteDirectory(dir);
	}

	@Test
	public void mergeFiles() throws Exception {
		// 创建随机的文件集合
		File tmpdir = randomFileGenerate();
		FileSystem system = FileSystem.get(new URI(FS), CONF, "root");
		FSDataOutputStream out = system.create(new Path("/usr/local/big-random-file.txt"));
		File[] files = tmpdir.listFiles();
		if (files != null) {
			for (File file : files) {
				InputStream in = new FileInputStream(file);
				IOUtils.copyBytes(in, out, 1024);
				out.write(10);
				out.flush();
				in.close();
			}
		}
		out.close();
		system.close();
		// 删除文件集合
		FileUtils.deleteDirectory(tmpdir);
	}
}
