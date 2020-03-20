package com.thirdlucky.storm.example.webcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Map;

public class WebCountSpout implements IRichSpout {
    BufferedReader bufferedReader;
    TopologyContext context;
    SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/website.log")));
            this.collector = spoutOutputCollector;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        try {
            String str;
            while ((str = bufferedReader.readLine()) != null) {
                collector.emit(new Values(str));
                Thread.sleep(500);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("webcount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
