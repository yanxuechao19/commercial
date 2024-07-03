package com.commercial.util.sink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;

import java.util.HashMap;
import java.util.Map;
/**
 * packageName com.commercial.util.sink
 *
 * @author yanxuechao
 * @version JDK 8
 * @className OpensearchSink
 * @date 2024/7/3
 * @description TODO
 */
public class OpensearchSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readTextFile("");//自己编的数据源类型

        // DataStream<String> input =  源代码

        input.sinkTo(
                new OpensearchSinkBuilder<String>()
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                        .setEmitter(
                                (element, context, indexer) ->
                                        indexer.add(createIndexRequest(element)))
                        .build());



    }
    private static IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .id(element)
                .source(json);
    }

}
