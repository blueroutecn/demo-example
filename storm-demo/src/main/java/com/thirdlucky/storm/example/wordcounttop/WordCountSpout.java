package com.thirdlucky.storm.example.wordcounttop;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class WordCountSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> list;
    private Random random;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        random = new Random(5);
        list = new ArrayList<String>();
        list.add("hello apache tomcat");
        list.add("hello apache spark");
        list.add("hello apache storm");
        list.add("hello apache hive");
        list.add("hello apache hadoop");
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(list.get(random.nextInt(5))));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
