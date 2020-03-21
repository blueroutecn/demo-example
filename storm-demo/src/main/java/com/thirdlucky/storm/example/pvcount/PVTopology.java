package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class PVTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("PVSpout",new PVSpout(),1);
        builder.setBolt("PVSessionBolt",new PVSessionBolt(),4).shuffleGrouping("PVSpout");
        builder.setBolt("PVSumBolt",new PVSumBolt(),1).shuffleGrouping("PVSessionBolt");

        Config config = new Config();
        config.setNumWorkers(1);

        try {
            if (args.length > 0) {
                StormSubmitter.submitTopology("PVTopology", config, builder.createTopology());
            } else {
                new LocalCluster().submitTopology("PVTopology", config, builder.createTopology());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
