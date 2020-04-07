package com.idreamsky.dc.blink.common.utils;

import lombok.Data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 不想把业务日志和 blink 的打到一起
 *
 * @author colby.luo
 * @date 2020/3/27 12:01
 */
public class GLogger {

    /**
     * 标准输出是同步的，多线程只会加剧竞争
     */
    private static final ExecutorService LOG_EXECUTOR_SERVICE = new ThreadPoolExecutor(
        1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(1000000),
        r -> {
            Thread t = new Thread(r, "global-stdout-log");
            t.setDaemon(true);
            return t;
        },
        new ThreadPoolExecutor.DiscardPolicy());


    private final String loggerName;

    public GLogger(String loggerName) {
        this.loggerName = loggerName;
    }

    public void info(String template, Object... objs) {
        LogEvent event = getLogEvent(template, objs);

        LOG_EXECUTOR_SERVICE.execute(() -> System.out.print(event));
    }

    public void error(String template, Object... objs) {
        Throwable t = null;

        if (objs != null && objs.length > 0) {
            Object last = objs[objs.length - 1];
            if (last instanceof Throwable) {
                t = (Throwable) last;
                objs = Arrays.copyOf(objs, objs.length - 1);
            }
        }
        LogEvent event = getLogEvent(template, objs);
        event.setThrowable(t);

        LOG_EXECUTOR_SERVICE.execute(() -> System.err.print(event));
    }

    private LogEvent getLogEvent(String template, Object[] objs) {
        Thread currentThread = Thread.currentThread();
        StackTraceElement traceElement = currentThread.getStackTrace()[3];

        LogEvent event = new LogEvent();
        event.setTime(Instant.now());
        event.setLoggerName(loggerName);
        event.setLineNum(traceElement.getLineNumber());
        event.setThread(currentThread.getName());

        String content = parseContent(template, objs);
        event.setContent(content);

        return event;
    }


    private String parseContent(String template, Object[] objs) {
        List<int[]> ijList = new ArrayList<>();

        int cursor = 0;
        while (cursor < template.length()) {
            int i = template.indexOf("{", cursor);
            if (i != -1) {
                int j = template.indexOf("}", i + 1);
                if (j == -1) {
                    cursor = i + 1;
                    continue;
                }
                ijList.add(new int[]{i, j});
                cursor = j + 1;
                continue;
            }
            break;
        }

        if (ijList.isEmpty()) {
            return template;
        }

        if (ijList.size() != objs.length) {
            IllegalArgumentException exception = new IllegalArgumentException(
                String.format("日志参数数量错误  template = [%s] paramNum = [%d]", template, objs.length));
            exception.printStackTrace();
            throw exception;
        }

        StringBuilder sb = new StringBuilder();
        int pos = 0;
        for (int k = 0; k < ijList.size(); k++) {
            int[] ij = ijList.get(k);
            Object value = objs[k];

            int i = ij[0];
            int j = ij[1];

            String head = template.substring(pos, i);

            pos = j + 1;

            sb.append(head);
            sb.append(value);
        }

        sb.append(template.substring(pos));
        return sb.toString();
    }

    @Data
    private static class LogEvent {
        public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        private Instant time;

        private String thread;

        private String loggerName;

        private Integer lineNum;

        private String content;

        private Throwable throwable;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            String name = loggerName;
            if (loggerName.length() > 35) {
                name = loggerName.substring(loggerName.length() - 35);
            }

            String tn = thread;
            if (thread.length() > 20) {
                tn = thread.substring(thread.length() - 20);
            }

            sb
                .append(time.atOffset(GlobalClock.ZONE_OFFSET).toLocalDateTime().format(FORMATTER)).append("|")
                .append(String.format("%-20s", tn)).append("|")
                .append(String.format("%-35s", name)).append(":")
                .append(String.format("%-3d", lineNum)).append(" --- ")
                .append(content);

            if (throwable != null) {
                StringWriter sw = new StringWriter();
                throwable.printStackTrace(new PrintWriter(sw));
                sb.append(System.lineSeparator()).append(sw);
            }

            return sb.append(System.lineSeparator()).toString();
        }
    }
}
