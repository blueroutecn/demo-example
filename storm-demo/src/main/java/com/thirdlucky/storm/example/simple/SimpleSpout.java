package com.thirdlucky.storm.example.simple;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.jetty.util.AtomicBiInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

public class SimpleSpout extends BaseRichSpout {
    private static Logger logger = LoggerFactory.getLogger(SimpleSpout.class);

    private TopologyContext context;
    private SpoutOutputCollector collector;
    private AtomicBiInteger ai;

    @Override
    public void open(Map<String, Object> map,
                     TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
        ai = new AtomicBiInteger();
        logger.warn("SimpleSpout.open=>hash:{},thread:{},task:{}", hashCode(), currentThread(), context.getThisTaskId());
    }

    @Override
    public void nextTuple() {
        long i = ai.getAndIncrement();
        if(i<=10000){
            logger.warn("SimpleSpout.nextTuple=>hash:{},thread:{},task:{}", hashCode(), currentThread(), context.getThisTaskId());
            collector.emit(new Values(i));
        }

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("i"));
    }

    @Override
    public void close() {
       logger.warn("SimpleSpout.close=>hash:{},thread:{},task:{}", hashCode(), currentThread(), context.getThisTaskId());
    }
}
