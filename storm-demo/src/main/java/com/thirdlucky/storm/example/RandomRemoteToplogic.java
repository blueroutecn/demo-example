package com.thirdlucky.storm.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class RandomRemoteToplogic {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomSpout", new RandomSpout());
        builder.setBolt("WrapStarBolt", new WrapStarBolt(), 4).shuffleGrouping("RandomSpout");
        builder.setBolt("WrapWellBolt", new WrapWellBolt(), 4).shuffleGrouping("RandomSpout");

        Config config = new Config();
        config.setNumWorkers(3);

        try {
            System.out.println("===================  submit =====================");
            StormSubmitter.submitTopology("RandomRemoteToplogic", config, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
