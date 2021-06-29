package com.percent.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * @author yunpeng.gu
 * @date 2021/1/4 22:50
 * @Email:yunpeng.gu@percent.cn
 */
@Data
@NonNull
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private long time;
    private Double tempd;
}
