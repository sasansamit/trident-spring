package com.kaseya.trident;

import static com.kaseya.trident.Utils.ValidateArgs;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public final class TopologySubmitter {

    private TopologySubmitter() {
    }

    public static void main(String[] args) {

        ValidateArgs(args);

        ApplicationContext appContext = new FileSystemXmlApplicationContext(args[0]);
//        ApplicationContext appContext = new ClassPathXmlApplicationContext(args[0]);

        ITopologySubmission submission = (ITopologySubmission) appContext
                .getBean("topologySubmission");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(submission.getTopologyId(),
                               submission.getConfig(),
                               submission.getTopology());

//        Utils.sleep(10000);
//        cluster.killTopology(submission.getTopologyId());
//        cluster.shutdown();
    }
}
