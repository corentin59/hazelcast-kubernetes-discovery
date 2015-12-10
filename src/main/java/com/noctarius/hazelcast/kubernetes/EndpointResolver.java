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

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public abstract class EndpointResolver {

    /**
     * Hazelcast service port.
     */
    private static final String HAZELCAST_SERVICE_PORT = "hazelcast-service-port";

    /**
     * The Hazelcast logger.
     */
    private final ILogger logger;

    /**
     * Default constructor.
     * @param logger is the logger.
     */
    public EndpointResolver(ILogger logger) {
        this.logger = logger;
    }

    /**
     * Resolve discovery nodes.
     * @return a list of discovery nodes
     */
    abstract List<DiscoveryNode> resolve();

    /**
     * Start.
     */
    abstract void start();

    /**
     * Destroy.
     */
    abstract void destroy();

    /**
     * Try to map a address by name.
     * @param address is the address name.
     * @return the address name if we found the host.
     */
    protected InetAddress mapAddress(final String address) {
        if(address != null) {
            try {
                return InetAddress.getByName(address);
            } catch (final UnknownHostException e) {
                logger.warning(String.format("Address '%s' could not be resolved", address));
            }
        }
        return null;
    }

    /**
     * Try to get the service port.
     * @param properties is the map of properties
     * @return the service port
     */
    protected int getServicePort(final Map<String, Object> properties) {
        int port = NetworkConfig.DEFAULT_PORT;
        if (properties != null) {
            final String servicePort = (String) properties.get(HAZELCAST_SERVICE_PORT);
            if (servicePort != null) {
                port = Integer.parseInt(servicePort);
            }
        }
        return port;
    }
}
