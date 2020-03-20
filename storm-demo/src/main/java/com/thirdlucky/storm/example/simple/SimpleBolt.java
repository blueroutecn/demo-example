package com.thirdlucky.storm.example.simple;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static java.lang.Thread.currentThread;

public class SimpleBolt extends BaseRichBolt {
    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleBolt.class);
    private TopologyContext context;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        logger.warn("SimpleBolt.prepare=>hash:{},thread:{},task:{}", hashCode(), currentThread(), context.getThisTaskId());
    }

    @Override
    public void execute(Tuple tuple) {
        Integer i = tuple.getIntegerByField("i");
        logger.warn("SimpleBolt.execute=>hash:{},thread:{},task:{},value:{}", hashCode(), currentThread(), context.getThisTaskId(),i);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        logger.warn("SimpleBolt.cleanup=>hash:{},thread:{},task:{}", hashCode(), currentThread(), context.getThisTaskId());
    }
}
