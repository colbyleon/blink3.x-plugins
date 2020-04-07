package com.idreamsky.dc.blink.sink.hdfs.pojo;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author colby.luo
 * @date 2020/3/26 17:12
 */
@Data
public class HdfsMessage {

    private String message;

    /**
     * 消息应该被写入哪里
     * 消息所属 path
     */
    private String path;
}
