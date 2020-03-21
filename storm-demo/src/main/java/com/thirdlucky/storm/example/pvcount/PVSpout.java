package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Map;

public class PVSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/website.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                collector.emit(new Values(line));
            }
        } catch (Exception ex) {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
