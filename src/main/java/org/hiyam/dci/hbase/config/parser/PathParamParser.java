package org.hiyam.dci.hbase.config.parser;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PathParamParser {

	private static String getTaskTimeAsString(long timestampMillis, String param) {
		if (StringUtils.isEmpty(param)) {
			return DateFormatUtils.format(timestampMillis, "yyyy-MM-dd");
		}
		String trimmedParam = param.trim().replaceAll(" +", " ");
		Integer start = trimmedParam.indexOf("-d ") + 3;
		Integer end = trimmedParam.indexOf(" ", start) == -1 ? trimmedParam.length() : trimmedParam.indexOf(" ", start);
		Integer dayAdd = 0 - Integer.valueOf(trimmedParam.substring(start, end));

		start = trimmedParam.indexOf("-h ") + 3;
		end = trimmedParam.indexOf(" ", start) == -1 ? trimmedParam.length() : trimmedParam.indexOf(" ", start);
		Integer hourAdd = 0 - Integer.valueOf(trimmedParam.substring(start, end));

		start = trimmedParam.indexOf("-f ") + 3;
		end = trimmedParam.length();
		String dateFormat = trimmedParam.substring(start, end);

		return DateFormatUtils.format(DateUtils.addDays(DateUtils.addHours(new Date(timestampMillis), hourAdd), dayAdd),
				dateFormat);
	}
	
	public static String getS3FullPath(String s3BasePath, List<Map<String, String>> pathParam) {
		if(StringUtils.isEmpty(s3BasePath)){
			return "";
		}
		StringBuffer fullPath = new StringBuffer(s3BasePath);
		if(!s3BasePath.endsWith("/")) {
			fullPath.append("/");
		}
		for (Map<String, String> param : pathParam) {
			String value = param.get("value");
			if("date".equalsIgnoreCase(param.get("type"))){
				value = getTaskTimeAsString(System.currentTimeMillis(), value);
			}
			
			String key = param.get("key");
			if(StringUtils.isEmpty(key)) {
				fullPath.append(value);
			} else {
				fullPath.append(key).append("=").append(value);
			}
			fullPath.append("/");
		}
		return fullPath.toString();
	}
}
