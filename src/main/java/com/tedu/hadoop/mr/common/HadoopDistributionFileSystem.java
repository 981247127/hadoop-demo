package com.tedu.hadoop.mr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HadoopDistributionFileSystem {
	public static boolean rm(String path) throws IOException, InterruptedException {
		Configuration configuration = ConfigurationManager.get();
		FileSystem system = FileSystem.get(ConfigurationManager.getDefaultFs(), configuration, "root");
		boolean deleted = system.delete(new Path(path), true);
		system.close();
		return deleted;
	}
}
