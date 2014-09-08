package com.github.fhuss.storm.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class to provide Hazelcast instance.
 *
 * @author Florian Hussonnois
 */
public class HazelcastProvider {

    public static final String HAZELCAST_INSTANCE = "storm-instance";

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastProvider.class);

    private HazelcastInstance hzInstance;

    private Config config;

    /**
     * Creates a new {@link HazelcastProvider} instance
     * that tries to find a usable XML configuration file.
     */
    public HazelcastProvider( ) {
        this(getXmlConf().setInstanceName(HAZELCAST_INSTANCE));
    }

    /**
     * Creates a new {@link HazelcastProvider} instance.
     *
     * @param instanceName the hazelcast instance name.
     * @throws IllegalArgumentException if the instance name of the config is null or empty.
     */
    public HazelcastProvider(int instanceName) {
        this(getXmlConf().setInstanceName(HAZELCAST_INSTANCE + "-" + instanceName));
    }

    /**
     * Constructs a XmlConfigBuilder that tries to find a usable XML configuration file.
     */
    private static Config getXmlConf() {
        return new XmlConfigBuilder().build();
    }
    /**
     * Creates a new {@link HazelcastProvider} instance.
     *
     * @throws NullPointerException if config is null.
     * @throws IllegalArgumentException if the instance name of the config is null or empty.
     */
    public HazelcastProvider(Config config) {
        if( config == null )
            throw new NullPointerException("Config cannot be null");
        if( config.getInstanceName() == null || config.getInstanceName().length() == 0)
            throw new IllegalArgumentException("The instance name of config is null or empty");
        this.config = config;
    }

    /**
     * Returns an hazelcast instance.
     */
    public HazelcastInstance getHzInstance() {
        if( hzInstance == null) {
            hzInstance = Hazelcast.getOrCreateHazelcastInstance(config);
            LOG.info("Get or create Hazelcast instance with name " + hzInstance.getName());
        }
        return hzInstance;
    }
}
