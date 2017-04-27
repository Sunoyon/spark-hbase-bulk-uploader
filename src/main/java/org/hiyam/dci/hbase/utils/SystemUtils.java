package org.hiyam.dci.hbase.utils;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class SystemUtils {

	public static void executeCommandSafely(String command) throws IOException {
		final Process syncProcess = Runtime.getRuntime().exec(command);
		Thread processDestroyThread = new Thread() {
			public void run() {
				if (syncProcess != null) {
					syncProcess.destroy();
				}
			}
		};
		Runtime.getRuntime().addShutdownHook(processDestroyThread);

		try {
			syncProcess.waitFor();
		} catch (Exception ex) {
		}

		List<String> errorLines = IOUtils.readLines(syncProcess.getErrorStream());
		if (errorLines != null && errorLines.size() > 0) {
		}
		Runtime.getRuntime().removeShutdownHook(processDestroyThread);
	}
	
	public static String getHdfsPath(String path) {
		if(StringUtils.isEmpty(path)) {
			return "";
		}
		if(path.startsWith("hdfs:///")) {
			return path;
		}
		if(path.startsWith("/")) {
			return "hdfs://" + path;
		}
		return "hdfs:///" + path;
	}
}
