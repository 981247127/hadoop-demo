package com.tedu.hadoop.mr.merge.m;

import com.tedu.hadoop.mr.merge.pojo.InfoWriteable;
import com.tedu.hadoop.mr.merge.pojo.Order;
import com.tedu.hadoop.mr.merge.pojo.Product;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MegreMapper extends Mapper<LongWritable, Text, Text, Text> {
	Map<String, String> map = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 1. 获取分布式缓存文件列表
		URI[] cacheFiles = context.getCacheFiles();
		// 2. 获取指定的分布式缓存文件的文件系统
		FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
		// 3. 获取缓存文件的输入流
		FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
		// 4. 读取文件内容，并将数据存入 Map 集合
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] strings = line.split(",");
			map.put(strings[0], line);
		}
		// reader.close();
		// fileSystem.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strings = value.toString().split(",");
		String pid = strings[2];
		System.out.println("=============================================");
		System.out.println("=============================================");
		System.out.println(map);
		System.out.println("=============================================");
		System.out.println("=============================================");
		String pline = map.get(pid);
		String vline = pline + "\t" + value.toString();
		context.write(new Text(pid), new Text(vline));
	}
}