package com.kaseya.trident;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.testing.FixedBatchSpout;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public final class TopologySubmitter {

    private TopologySubmitter() {
    }

    private static void validateArgs(final String[] args) {

        if (args.length < 2) {
            throw new IllegalArgumentException("Arguments: Needs two arguments - <XmlApplicationContext> <TopologySubmission bean name>");
        }
        else if (args[0] == null) {
            throw new IllegalArgumentException("Argument 1: XmlApplicationContext was not defined");
        } else if (args[1] == null) {
            throw new IllegalArgumentException("Argument 2: TopologySubmission bean was not defined");
        }
    }

    public static void main(String[] args) {

        validateArgs(args);
        ApplicationContext appContext = new ClassPathXmlApplicationContext(args[0]);

        SingleTopologySubmission submission = (SingleTopologySubmission) appContext
                .getBean("wordCountTopologySubmission");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(submission.getTopologyId(), submission.getConfig(), submission.getTopology().build());
        
        Utils.sleep(10000);
        cluster.killTopology(submission.getTopologyId());
        cluster.shutdown();
    }
}
