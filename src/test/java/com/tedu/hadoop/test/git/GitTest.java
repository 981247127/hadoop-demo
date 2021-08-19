package com.tedu.hadoop.test.git;

import com.tedu.hadoop.mr.common.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class GitTest {
	@Test
	public void testConfiguration() {
		Configuration configuration = ConfigurationManager.get();
		System.out.println(configuration.get("fs.defaultFS", "file://"));
		System.out.println("wrote by hot-fix");
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("modified by master");
	}
}