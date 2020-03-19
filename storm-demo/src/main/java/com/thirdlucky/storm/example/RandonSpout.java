package com.thirdlucky.storm.example;

import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.concurrent.TimeUnit;

import java.util.Map;

import static org.apache.storm.shade.io.netty.util.internal.ThreadLocalRandom.current;


public class RandonSpout extends BaseRichSpout {

    private static Map<Integer, String> map = new HashedMap();
    private SpoutOutputCollector collector;

    public RandonSpout() {
        map.put(0, "Kafka");
        map.put(1, "Spark");
        map.put(2, "Flink");
        map.put(3, "Storm");
        map.put(4, "NIFI");
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("open==========");
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(map.get(current().nextInt(5))));
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("stream"));
    }
}
