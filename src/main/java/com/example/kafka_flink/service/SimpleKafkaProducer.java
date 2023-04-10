package com.example.kafka_flink.service;

import com.example.kafka_flink.util.MyNoParalleSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Service
public class SimpleKafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        DataStreamSource<String> source=env.addSource(new MyNoParalleSource()).setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        FlinkKafkaProducer<String> producer=new FlinkKafkaProducer<String>(
                "127.0.0.1:9092","test",new SimpleStringSchema());
        source.addSink(producer);
        env.execute();
    }

    //@PostConstruct
    public void producer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        DataStreamSource<String> source=env.addSource(new MyNoParalleSource()).setParallelism(1);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        FlinkKafkaProducer<String> producer=new FlinkKafkaProducer<String>(
                "127.0.0.1:9092","test",new SimpleStringSchema());
        source.addSink(producer);
        env.execute();
    }

}
