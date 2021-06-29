package com.percent.time.assinger;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author yunpeng.gu
 * @date 2021/1/26 18:12
 * @Email:yunpeng.gu@percent.cn
 */
public class MyAssinger implements AssignerWithPeriodicWatermarks {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Override
    public long extractTimestamp(Object element, long previousElementTimestamp) {
        return 0;
    }
}
