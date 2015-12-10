/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.noctarius.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.*;

/**
 * Just the factory to create the Kubernetes Discovery Strategy
 */
public class HazelcastKubernetesDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

    /**
     * Property definition for configuration.
     */
    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        final List<PropertyDefinition> propertyDefinitions = new ArrayList<PropertyDefinition>(4);
        propertyDefinitions.add(KubernetesProperties.SERVICE_DNS);
        propertyDefinitions.add(KubernetesProperties.SERVICE_DNS_IP_TYPE);
        propertyDefinitions.add(KubernetesProperties.SERVICE_NAME);
        propertyDefinitions.add(KubernetesProperties.NAMESPACE);
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(propertyDefinitions);
    }

    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return HazelcastKubernetesDiscoveryStrategy.class;
    }

    /**
     * Try to found the good discovery strategie.
     * @param discoveryNode is the discovery node
     * @param logger is the logger
     * @param properties are properties
     * @return a discovery strategy implementation for kubernetes
     */
    public DiscoveryStrategy newDiscoveryStrategy(final DiscoveryNode discoveryNode, final ILogger logger, final Map<String, Comparable> properties) {
        return new HazelcastKubernetesDiscoveryStrategy(logger, properties);
    }

    /**
     * Get all properties definitions.
     * @return
     */
    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }
}
