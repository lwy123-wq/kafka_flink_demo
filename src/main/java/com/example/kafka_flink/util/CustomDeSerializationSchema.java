package com.example.kafka_flink.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CustomDeSerializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String,String>> {

    //是否表示流的最后一条元素,设置为false，表示数据会源源不断地到来
    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> stringStringConsumerRecord) {
        return false;
    }
    //这里返回一个ConsumerRecord<String,String>类型的数据，除了原数据还包括topic，offset，partition等信息
    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return new ConsumerRecord<String,String>(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                new String(consumerRecord.key()),
                new String(consumerRecord.value())
        );
    }
    //指定数据的输入类型
    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }
}
