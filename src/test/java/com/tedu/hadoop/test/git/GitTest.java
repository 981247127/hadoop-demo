package com.tedu.hadoop.test.git;

import com.tedu.hadoop.mr.common.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;

public class GitTest {
	public static void main(String[] args) {
		Configuration configuration = ConfigurationManager.get();
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("wrote by hot-fix");
	}
}
