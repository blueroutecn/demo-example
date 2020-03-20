package com.thirdlucky.storm.example.webcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WebCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WebCountSpout", new WebCountSpout(), 1);
        builder.setBolt("WebCountBolt", new WebCountBolt(), 1).shuffleGrouping("WebCountSpout");

        Config config = new Config();
        config.setNumWorkers(2);

        if (args.length > 0) {
            StormSubmitter.submitTopology("webcount", config, builder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("website",config,builder.createTopology());
        }
    }
}
