package com.example.kafka_flink.service;

import com.example.kafka_flink.util.CustomDeSerializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class SimpleKafkaConsumer {

    public static void main(String[] args) throws Exception {
         SimpleKafkaConsumer.consumer();
    }

    //@PostConstruct
    public static void consumer() throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id","group_test");
        FlinkKafkaConsumer<String> consumer=new FlinkKafkaConsumer(
                "test",new SimpleStringSchema(),properties);
        //FlinkKafkaConsumer<String> consumer=new FlinkKafkaConsumer("test",new CustomDeSerializationSchema(),properties);
        consumer.setStartFromEarliest();
        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                System.out.println(s);
            }
        });
        env.execute("start consumer...");
    }

    //消费多个topic
    public static void consumerTopic() throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.server","127.0.0.1:9092");
        properties.setProperty("group.id","group_test");
        List<String> topics=new ArrayList<>();
        topics.add("test_A");
        topics.add("test_B");
        FlinkKafkaConsumer<Tuple2<String, String>> consumer = new FlinkKafkaConsumer(topics, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        env.addSource(consumer).flatMap(new FlatMapFunction<Tuple2<String, String>, Object>() {
            @Override
            public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<Object> collector) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
        env.execute("start consumer...");
    }

}
