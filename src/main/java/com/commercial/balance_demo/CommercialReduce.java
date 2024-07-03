package com.commercial.balance_demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.commercial.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.text.DecimalFormat;


/**
 * packageName com.commercial.balance_demo
 *
 * @author yanxuechao
 * @version JDK 8
 * @className CommercialReduce
 * @date 2024/7/2
 * @description TODO
 */
public class CommercialReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建一个DecimalFormat对象，设置保留四位小数
        DecimalFormat df = new DecimalFormat("#.####");
        // TODO 1. 读取业务主流
        String topic = "growalong_prod";
        String groupId = "growalong_prod_0702";
        //DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        DataStreamSource<String> coinDs = env.readTextFile("D:\\code\\commercial\\input\\user_coin_log.txt");

        // TODO 2. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = coinDs.map( JSON::parseObject);

        // TODO 3. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                jsonObj ->
                {
                    try {

                        if(jsonObj.getString("table").equals("user_coin_wallet_log")  && jsonObj.getJSONObject("data").getString("change_type").equals("coin_recharge")) {
                            return true;
                        }
                        return false;
                    } catch (JSONException jsonException) {
                        return false;
                    }
                });
        filterDS.print();
        // TODO 4. 主流获取需要的字段
        SingleOutputStreamOperator<Double> mapstram = filterDS.map(new MapFunction<JSONObject, Double>() {
            @Override
            public Double map(JSONObject value) throws Exception {
                return value.getJSONObject("data").getDouble("balance");
            }
        });
        // TODO 5. 对获取的字段进行聚合
        SingleOutputStreamOperator<Double> rs = mapstram.keyBy(key -> true).reduce(new ReduceFunction<Double>() {

            @Override
            public Double reduce(Double value1, Double value2) throws Exception {

                return ((((value1/0.9)/100-((value1/0.9)/100*0.3*0.3))-(((value1/0.9)/100-((value1/0.9)/100*0.3*0.3))*0.95)) - (value1/0.9)/100*0.3*(1-0.3)) + ((((value2/0.9)/100-((value2/0.9)/100*0.3*0.3))-(((value2/0.9)/100-((value2/0.9)/100*0.3*0.3))*0.95)) - (value2/0.9)/100*0.3*(1-0.3));
            }
        });
        rs.print();



        env.execute();
    }
}


