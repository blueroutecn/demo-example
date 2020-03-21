package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PVSumBolt extends BaseRichBolt {
    private Map<Long, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        Long thread = input.getLongByField("thread");
        Integer count = input.getIntegerByField("count");
        map.put(thread, count);

        final AtomicInteger sum = new AtomicInteger();
        StringBuffer stringBuffer = new StringBuffer();

        map.forEach((k, v) -> {
            sum.addAndGet(v);
            stringBuffer.append("[" + k + "=" + v + "]\t");
        });

        System.out.println(stringBuffer.toString() + "\tcount:" + sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
