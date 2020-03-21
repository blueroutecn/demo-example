package com.thirdlucky.storm.example.wordcounttop;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WordCountSpout", new WordCountSpout(), 1);
        builder.setBolt("WordCountSplitBolt",new WordCountSplitBolt(),1).shuffleGrouping("WordCountSpout");
        builder.setBolt("WordCountSumBold", new WordCountSumBold(), 2).shuffleGrouping("WordCountSplitBolt");

        Config config = new Config();
        config.setNumWorkers(1);


        try {
            if (args.length > 0) {
                StormSubmitter.submitTopology("WordCount", config, builder.createTopology());
            } else {
                new LocalCluster().submitTopology("WordCount", config, builder.createTopology());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
