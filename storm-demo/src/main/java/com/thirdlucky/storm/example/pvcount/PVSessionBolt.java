package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PVSessionBolt extends BaseRichBolt {
    private Map<Long, Integer> map = new HashMap<>();
    private OutputCollector collector;
    private AtomicInteger count;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        count = new AtomicInteger();
    }

    @Override
    public void execute(Tuple input) {
//        www.thirdlucky.com	1:ABYH6Y4V4SCVXTG6DPB4VH9U123	2014-01-07 11:40:49	192.168.227.135
        String line = input.getStringByField("line");
        String[] split = line.split("\t");
        if (split != null && split.length == 4) {
            String session = split[1];
            collector.emit(new Values(Thread.currentThread().getId(), count.incrementAndGet()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("thread", "count"));
    }
}
