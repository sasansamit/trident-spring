package com.kaseya.kafka;

import org.springframework.beans.factory.FactoryBean;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TridentKafkaConfig;

public class TridentKafkaConfigFactoryBean implements
        FactoryBean<TridentKafkaConfig> {

    private BrokerHosts _hosts;
    private String _topic;

    public TridentKafkaConfigFactoryBean(final BrokerHosts hosts,
                                         final String topic) {
        this._hosts = hosts;
        this._topic = topic;
    }

    @Override
    public TridentKafkaConfig getObject() throws Exception {
        TridentKafkaConfig config = new TridentKafkaConfig(_hosts, _topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        return config;
    }

    @Override
    public Class<TridentKafkaConfig> getObjectType() {
        return TridentKafkaConfig.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

}
