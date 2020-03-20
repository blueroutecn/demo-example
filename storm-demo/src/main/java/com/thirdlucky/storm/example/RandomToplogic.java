package com.thirdlucky.storm.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import java.util.concurrent.TimeUnit;

public class RandomToplogic {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("RandomSpout",new RandomSpout());
        builder.setBolt("WrapStarBolt",new WrapStarBolt()).shuffleGrouping("RandomSpout");
        builder.setBolt("WrapWellBolt",new WrapWellBolt()).shuffleGrouping("RandomSpout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RandomToplogic",config,builder.createTopology());

        System.out.println("start to running...");

        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("RandomToplogic");
        cluster.shutdown();
    }
}
