package com.idreamsky.dc.blink.sink.hdfs.vo;

import lombok.Getter;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author colby.luo
 * @date 2020/3/26 18:00
 */
@ToString
public class PathSchema {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{(\\w[\\w|\\d]*)}");

    private final String pathSchemaStr;

    @Getter
    private Map<String, String> placeholderMap;

    public PathSchema(String pathSchemaStr) {
        this.pathSchemaStr = pathSchemaStr;

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(pathSchemaStr);

        placeholderMap = new HashMap<>();

        while (matcher.find()) {
            String placeholder = matcher.group();
            String param = matcher.group(1);
            placeholderMap.put(param, placeholder);
        }
    }

    public PathBuilder builder() {
        return new PathBuilder(this.pathSchemaStr);
    }

    public class PathBuilder {
        private final String pathSchemaStr;

        private PathBuilder(String pathSchemaStr) {
            this.pathSchemaStr = pathSchemaStr;
        }

        public PathBuilder fill(String name, String value) {
            String placeholder = placeholderMap.get(name);
            String newStr = pathSchemaStr.replace(placeholder, value);
            return new PathBuilder(newStr);
        }

        public String build() {
            Collection<String> placeholders = placeholderMap.values();
            for (String placeholder : placeholders) {
                if (pathSchemaStr.contains(placeholder)) {
                    throw new IllegalStateException(String.format("路径错误，path = [%s]", pathSchemaStr));
                }
            }
            return pathSchemaStr;
        }
    }
}
