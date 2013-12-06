package com.kaseya.trident;

import static com.kaseya.trident.Utils.ValidateArgs;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public final class TopologySubmitter {

    private TopologySubmitter() {
    }

    public static void main(String[] args) {

        ValidateArgs(args);

        ApplicationContext appContext = new ClassPathXmlApplicationContext(args[0]);

        SingleTopologySubmission submission = (SingleTopologySubmission) appContext
                .getBean("wordCountTopologySubmission");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(submission.getTopologyId(),
                               submission.getConfig(),
                               submission.getTopology().build());

        Utils.sleep(10000);
        cluster.killTopology(submission.getTopologyId());
        cluster.shutdown();
    }
}
