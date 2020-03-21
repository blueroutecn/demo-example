package com.thirdlucky.storm.example.pvcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class PVTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("PVSpout", new PVSpout(), 1);
//      1. PV 计算
//        builder.setBolt("PVSessionBolt",new PVSessionBolt(),4).shuffleGrouping("PVSpout");
//        builder.setBolt("PVSumBolt",new PVSumBolt(),1).shuffleGrouping("PVSessionBolt");
//      2. UV 计算
        builder.setBolt("UVSessionBolt", new UVSessionBolt(), 4).shuffleGrouping("PVSpout");
        builder.setBolt("UVSumBolt", new UVSumBolt(), 1).shuffleGrouping("UVSessionBolt");

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
