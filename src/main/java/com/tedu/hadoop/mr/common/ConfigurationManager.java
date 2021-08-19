package com.tedu.hadoop.mr.common;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ConfigurationManager {
	private static final Configuration configuration = new Configuration();

	static {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties");
		Properties properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Map.Entry<Object, Object> entry : properties.entrySet()) {
			Optional.ofNullable(entry.getKey()).ifPresent(k -> {
				Optional.ofNullable(entry.getValue()).ifPresent(v -> configuration.set(k.toString(), v.toString()));
			});
		}
	}

	public static Configuration get() {
		return configuration;
	}

	public static URI getDefaultFs() {
		try {
			return new URI(configuration.get("fs.defaultFS"));
		} catch (URISyntaxException e) {
		}
		return null;
	}
}