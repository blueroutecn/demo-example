package com.thirdlucky.storm.example.simple;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleToplogic {
    private static Logger logger = LoggerFactory.getLogger(SimpleToplogic.class);

    /**
     * 1. top name
     * 2. prefix
     * 3. worker number
     * 4. spout hit
     * 5. spout task
     * 6. bolt hit
     * 7. holt task
     */

    public static void main(String[] args) {
        if (args.length < 7) {
            throw new IllegalArgumentException("must 7 args");
        }
        TopArgs ops = TopArgs.build(args);
        logger.warn("SimpleToplogic-ops:{}", ops);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(ops.prefix + "-SimpleSpout", new SimpleSpout(), ops.spoutHits).setNumTasks(ops.spoutTasks);
        builder.setBolt(ops.prefix + "-SimpleBolt", new SimpleBolt(), ops.boldHits).setNumTasks(ops.boldTasks).shuffleGrouping(ops.prefix + "-SimpleSpout");

        Config config = new Config();
        config.setNumWorkers(ops.works);
        try {
            StormSubmitter.submitTopology(ops.prefix + "-SimpleToplogic", config, builder.createTopology());
            logger.warn("==============================================================");
            logger.warn("{} is submiteed!", ops.prefix + ops.topName);
            logger.warn("==============================================================");
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            e.printStackTrace();
        }
    }

    static class TopArgs {
        String topName;
        String prefix;
        int works;
        int spoutHits;
        int spoutTasks;
        int boldHits;
        int boldTasks;

        public TopArgs(String topNAme, String prefix, int works, int spoutHits, int spoutTasks, int boldHits, int boldTasks) {
            this.topName = topNAme;
            this.prefix = prefix;
            this.works = works;
            this.spoutHits = spoutHits;
            this.spoutTasks = spoutTasks;
            this.boldHits = boldHits;
            this.boldTasks = boldTasks;
        }

        static TopArgs build(String[] args) {
            return new TopArgs(args[0], args[1], Integer.parseInt(args[02]), Integer.parseInt(args[03]), Integer.parseInt(args[04]), Integer.parseInt(args[05]), Integer.parseInt(args[06]));
        }
    }
}
