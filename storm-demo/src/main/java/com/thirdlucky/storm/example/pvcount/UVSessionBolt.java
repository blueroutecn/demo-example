package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;

public class UVSessionBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashSet<String> set = new HashSet<>();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String[] split = line.split("\t");

        if (split != null && split.length >= 4) {
            String key = split[3];
            if (!set.contains(key)) {
                set.add(key);
                collector.emit(new Values(key));
                System.out.println("find new ip:" + key);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip"));
    }
}
