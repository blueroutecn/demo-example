package com.thirdlucky.storm.example.webcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WebCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WebCountSpout", new WebCountSpout(), 1);
//        builder.setBolt("WebCountBolt", new WebCountBolt(), 2).shuffleGrouping("WebCountSpout");
//        builder.setBolt("WebCountBolt", new WebCountBolt(), 2).fieldsGrouping("WebCountSpout", new Fields("webcount"));
//        builder.setBolt("WebCountBolt", new WebCountBolt(), 2).allGrouping("WebCountSpout");
        builder.setBolt("WebCountBolt", new WebCountBolt(), 2).globalGrouping("WebCountSpout");

        Config config = new Config();
        config.setNumWorkers(1);

        if (args.length > 0) {
            StormSubmitter.submitTopology("webcount", config, builder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("website", config, builder.createTopology());
        }
    }
}
