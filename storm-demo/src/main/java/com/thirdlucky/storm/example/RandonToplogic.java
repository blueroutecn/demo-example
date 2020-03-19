package com.thirdlucky.storm.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import java.util.concurrent.TimeUnit;

public class RandonToplogic {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("RandonSpout",new RandonSpout());
        builder.setBolt("WrapStarBolt",new WrapStarBolt()).shuffleGrouping("RandonSpout");
        builder.setBolt("WrapWellBolt",new WrapWellBolt()).shuffleGrouping("RandonSpout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RandonToplogic",config,builder.createTopology());

        System.out.println("start to running...");

        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("RandonToplogic");
        cluster.shutdown();
    }
}
