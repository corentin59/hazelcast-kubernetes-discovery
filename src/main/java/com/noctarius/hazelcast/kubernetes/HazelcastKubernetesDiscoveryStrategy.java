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
import com.hazelcast.util.StringUtil;

import java.util.Map;

import static com.noctarius.hazelcast.kubernetes.KubernetesProperties.*;

final class HazelcastKubernetesDiscoveryStrategy implements DiscoveryStrategy {

    /**
     * End point resolver.
     */
    private final EndpointResolver endpointResolver;

    /**
     * Default class constructor.
     * @param logger is the Hazelcast logger
     * @param properties are properties.
     */
    public HazelcastKubernetesDiscoveryStrategy(final ILogger logger, final Map<String, Comparable> properties) {
        // Try to extract configuration
        final String serviceDns = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS);
        final IpType serviceDnsIpType = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS_IP_TYPE, IpType.IPV4);
        final String serviceName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_NAME);
        final String namespace = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, NAMESPACE);

        // Service DNS is not null, but the user has not specified the service name or the name space.
        if (serviceDns != null && (serviceName == null || namespace == null)) {
            throw new RuntimeException("For kubernetes discovery either 'service-dns' or 'service-name' and 'namespace' must be set");
        }

        logger.info(String.format("Kubernetes Discovery properties: {service-dns: %s, service-dns-ip-type: %s, service-name: %s, namespace: %s}", serviceDns, serviceDnsIpType.name(), serviceName, namespace));

        /*
         * Call the good discovery strategy :
         * - Service DNS
         * - Kubernetes API
         */
        if (serviceDns != null) {
            this.endpointResolver = new DnsEndpointResolver(logger, serviceDns, serviceDnsIpType);
        } else {
            this.endpointResolver = new ServiceEndpointResolver(logger, serviceName, namespace);
        }
        logger.info("Kubernetes Discovery activated resolver: " + this.endpointResolver.getClass().getSimpleName());
    }

    /**
     * The <tt>start</tt> method is used to initialize internal state and perform any kind of
     * startup procedure like multicast socket creation. The behavior of this method might
     * change based on the {@link DiscoveryNode} instance passed to the {@link DiscoveryStrategyFactory}.
     */
    public void start() {
        endpointResolver.start();
    }

    /**
     * Returns a set of all discovered nodes based on the defined properties that were used
     * to create the <tt>DiscoveryStrategy</tt> instance.
     * @return a set of all discovered nodes
     */
    public Iterable<DiscoveryNode> discoverNodes() {
        return endpointResolver.resolve();
    }

    /**
     * The <tt>stop</tt> method is used to stop internal services, sockets or to destroy any
     * kind of internal state.
     */
    public void destroy() {
        endpointResolver.destroy();
    }

    /**
     * Try to get the value, if we can't found we return null.
     * @param properties are properties
     * @param prefix is the prefix
     * @param property is the target property.
     * @param <T> the type
     * @return the value with type
     */
    private <T extends Comparable> T getOrNull(final Map<String, Comparable> properties, final String prefix, final PropertyDefinition property) {
        return getOrDefault(properties, prefix, property, null);
    }

    /**
     * Try to get a property or the default value.
     * @param properties are properties
     * @param prefix is the prefix
     * @param property is the target property
     * @param defaultValue the default value to apply if not found
     * @param <T> the value with type
     * @return the property
     */
    private <T extends Comparable> T getOrDefault(final Map<String, Comparable> properties, final String prefix, final PropertyDefinition property, final T defaultValue) {
        if (property == null) {
            return defaultValue;
        }

        Comparable value = readProperty(prefix, property);
        if (value == null) {
            value = properties.get(property.key());
        }

        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }

    /**
     * Read the property.
     * @param prefix is the prefix
     * @param property is the property
     * @return a comparable
     */
    private Comparable readProperty(final String prefix, final PropertyDefinition property) {
        if (prefix != null) {
            final String p = getProperty(prefix, property);
            String v = System.getProperty(p);
            if (StringUtil.isNullOrEmpty(v)) {
                v = System.getenv(p);
                if (StringUtil.isNullOrEmpty(v)) {
                    v = System.getenv(cIdentifierLike(p));
                }
            }

            if (!StringUtil.isNullOrEmpty(v)) {
                return property.typeConverter().convert(v);
            }
        }
        return null;
    }

    /**
     * Clear the property.
     * @param property is the property
     * @return the property in a good string state.
     */
    private String cIdentifierLike(String property) {
        property = property.toUpperCase();
        property = property.replace(".", "_");
        return property.replace("-", "_");
    }

    /**
     * Get a property with prefix.
     * @param prefix is the prefix
     * @param property is the property.
     * @return the property.
     */
    private String getProperty(String prefix, PropertyDefinition property) {
        StringBuilder sb = new StringBuilder(prefix);
        if (prefix.charAt(prefix.length() - 1) != '.') {
            sb.append('.');
        }
        return sb.append(property.key()).toString();
    }
}
