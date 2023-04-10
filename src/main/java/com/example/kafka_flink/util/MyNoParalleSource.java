package com.example.kafka_flink.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyNoParalleSource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning){
            List<String> list=new ArrayList<>();
            list.add("aaaaaa");
            list.add("bbbbbbbbbbb");
            list.add("cccccccccc");
            list.add("ddddddddd");
            int result=new Random().nextInt(4);
            sourceContext.collect(list.get(result));
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
