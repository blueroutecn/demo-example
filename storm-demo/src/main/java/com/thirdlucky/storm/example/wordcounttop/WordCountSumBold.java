package com.thirdlucky.storm.example.wordcounttop;

import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WordCountSumBold extends BaseRichBolt {
    private int count;
    private Map<String, Integer> map;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = new HashedMap();
        count = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getStringByField("word");
        if (map.containsKey(key)) {
            Integer count = map.get(key);
            map.put(key, count + 1);
        } else {
            map.put(key, 1);
        }

        StringBuffer buffer = new StringBuffer();

        map.forEach((k, v) -> {
            buffer.append("[" + k + "=" + v + "]");
        });
        count++;
        System.out.println(Thread.currentThread().getId() + "\t" + buffer.toString() + "\tline:" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
