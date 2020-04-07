package com.idreamsky.dc.blink.common.utils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * @author colby.luo
 * @date 2020/3/26 18:32
 */
public class GlobalClock {


    public static final ZoneOffset ZONE_OFFSET = ZoneOffset.ofHours(8);
    /**
     * 秒级时钟
     */
    private static final Clock CLOCK = Clock.tickSeconds(ZONE_OFFSET);

    public static Instant currentSecond() {
        return CLOCK.instant();
    }
}
