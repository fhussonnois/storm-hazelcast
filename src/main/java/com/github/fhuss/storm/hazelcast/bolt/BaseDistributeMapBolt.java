package com.github.fhuss.storm.hazelcast.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import com.github.fhuss.storm.hazelcast.HazelcastProvider;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;

import java.util.Collection;
import java.util.Map;

/**
 * Default {@link BaseRichBolt} for querying/listing on a specified distributed map.
 *
 * @author Florian Hussonnois
 */
public abstract class BaseDistributeMapBolt<K, V> extends BaseRichBolt implements EntryListener<K, V> {

    private String name;
    private HazelcastInstance hzInstance;

    /**
     * Creates a new {@link BaseDistributeMapBolt} instance.
     * @param name name of the distributed map.
     */
    public BaseDistributeMapBolt(String name) {
        this.name = name;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        HazelcastProvider provider = new HazelcastProvider(context.getThisWorkerPort());
        this.hzInstance = provider.getHzInstance();
        IMap<K, V> distributedMap = getDistributedMap();
        distributedMap.addEntryListener(this, true);
    }

    /**
     * Returns the distributed map.
     * @return a IMap<K, V>
     */
    public IMap<K, V> getDistributedMap() {
        return hzInstance.getMap(name);
    }

    /**
     * Queries the map based on the specified predicate and returns the values of matching entries.
     * @param predicate query criteria
     * @return result value collection of the query
     */
    public Collection<V> query(SqlPredicate predicate) {
        return getDistributedMap().values(predicate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void entryAdded(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void entryRemoved(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void entryUpdated(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void entryEvicted(EntryEvent<K, V> event) {
        onEntryEvent(event);
    }
    /**
     * This method is called when an one of the methods of the {@link com.hazelcast.core.EntryListener} is not
     * overridden. It can be practical if you want to bundle some/all of the methods to a single method.
     *
     * @param event the EntryEvent.
     *
     * @see com.hazelcast.core.EntryAdapter#onEntryEvent(com.hazelcast.core.EntryEvent).
     */
    public abstract void onEntryEvent(EntryEvent<K, V> event);
}
