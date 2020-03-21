package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.Map;

public class UVSumBolt extends BaseRichBolt {
    HashSet<String> set = new HashSet<>();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String ip = input.getStringByField("ip");
        if(!set.contains(ip)){
            set.add(ip);
            System.out.println("Count is:" + set.size() + "\tNew ip:" + ip);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
