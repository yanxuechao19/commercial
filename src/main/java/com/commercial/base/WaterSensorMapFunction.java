package com.commercial.base;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * packageName com.commercial.base
 *
 * @author yanxuechao
 * @version JDK 8
 * @className WaterSensorMapFunction
 * @date 2024/7/4
 * @description TODO
 */
public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0],Long.valueOf(datas[1]) ,Integer.valueOf(datas[2]) );
    }
}
