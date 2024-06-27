package com.commercial.Richfunction;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<Integer> maprich = env.fromElements(1, 2, 3, 4).map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value +1;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });
        maprich.print();
        env.execute();

    }
}
