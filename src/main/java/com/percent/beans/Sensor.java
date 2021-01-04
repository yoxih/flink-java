package com.percent.beans;

import lombok.*;

/**
 * @author yunpeng.gu
 * @date 2021/1/4 22:50
 * @Email:yunpeng.gu@percent.cn
 */
@Data
@NonNull
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {
    private String name;
    private long time;
    private String tempd;
}
