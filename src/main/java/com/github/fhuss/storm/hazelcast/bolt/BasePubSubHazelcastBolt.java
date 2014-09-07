package com.github.fhuss.storm.hazelcast.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import com.github.fhuss.storm.hazelcast.HazelcastProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;

import java.util.Map;

/**
 * Default BaseRichBolt using Hazelcast to publish/subscribe on specific topic name.
 *
 * @author Florian Hussonnois
 */
public abstract class BasePubSubHazelcastBolt<T> extends BaseRichBolt implements MessageListener<T> {

    private String name;
    private HazelcastInstance hzInstance;

    /**
     * Creates a new {@link BasePubSubHazelcastBolt} instance.
     * @param name name of the topic on which this bolt have to listen.
     */
    public BasePubSubHazelcastBolt(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        HazelcastProvider provider = new HazelcastProvider(context.getThisWorkerPort());
        this.hzInstance = provider.getHzInstance();
        ITopic<T> topic = hzInstance.getTopic(name);
        topic.addMessageListener(this);
    }

    /**
     * Sends a message object to a specified topic name.
     *
     * @param name name of the topic on which message have to be send
     * @param message the message which have to be send
     * @param <E> message type
     */
    public <E> void sendMessage(String name, E message) {
        ITopic<E> topic = hzInstance.getTopic(name);
        topic.publish(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        hzInstance.shutdown();
    }
}
