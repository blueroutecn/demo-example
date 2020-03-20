package com.thirdlucky.storm.example.webcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WebCountBolt implements IRichBolt {
    private int i=0;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String webcount = tuple.getStringByField("webcount");
        String[] split = webcount.split("\t");
        String session_id = split[1];
        i++;
        System.out.println(Thread.currentThread().getId() + "\tsession_id:" + session_id + "line_num:" + i);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
