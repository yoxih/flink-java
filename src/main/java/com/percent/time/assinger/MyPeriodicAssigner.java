package com.percent.time.assinger;

import com.percent.beans.SensorReading;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author yunpeng.gu
 * @date 2021/1/26 18:23
 * @Email:yunpeng.gu@percent.cn
 */
public class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

    // 延迟一分钟
    private long bound = 60 * 1000L;
    // 当前最大水位线
    private long maxTs = Long.MAX_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }


    @Override
    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
        // 如果大于最大水位线，则修改最大水位线
        maxTs = Math.max(element.getTime(),this.maxTs);
        return element.getTime();
    }
}
