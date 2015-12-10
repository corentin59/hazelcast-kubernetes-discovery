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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class ServiceEndpointResolver extends EndpointResolver {

    /**
     * A Kubernetes Service is an abstraction which defines a logical set of Pods.
     */
    private final String serviceName;

    /**
     * Kubernetes supports multiple virtual clusters backed by the same physical cluster.
     * These virtual clusters are called namespaces.
     */
    private final String namespace;

    /**
     * Kubernetes client.
     */
    private final KubernetesClient client;

    /**
     * Default constructor.
     * @param logger is the logger
     * @param serviceName is the service name for pods discovery
     * @param namespace is the namespace for pods discovery
     */
    public ServiceEndpointResolver(final ILogger logger, final String serviceName, final String namespace) {
        super(logger);

        this.serviceName = serviceName;
        this.namespace = namespace;

        // Extract account token
        final String accountToken = getAccountToken();
        logger.info(String.format("Kubernetes Discovery: Bearer Token { %s }", accountToken));

        // Build a Kubernetes client to access REST API.
        this.client = new DefaultKubernetesClient(new ConfigBuilder().withOauthToken(accountToken).build());
    }

    List<DiscoveryNode> resolve() {
        // Try to get endpoints in a namespace and the service name...
        final Endpoints endpoints = client.endpoints().inNamespace(namespace).withName(serviceName).get();

        // Try to win time, if we haven't endpoints we return quickly an empty collection.
        if (endpoints == null) {
            return Collections.emptyList();
        }

        final List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for(final EndpointSubset endpointSubset : endpoints.getSubsets()) {
            for(final EndpointAddress endpointAddress : endpointSubset.getAddresses()) {
                final Map<String, Object> properties = endpointAddress.getAdditionalProperties();
                final InetAddress inetAddress = mapAddress(endpointAddress.getIp());
                final Address address = new Address(inetAddress, getServicePort(properties));
                discoveredNodes.add(new SimpleDiscoveryNode(address, properties));
            }
        }

        return discoveredNodes;
    }

    @Override
    void start() {}

    @Override
    void destroy() {
        client.close();
    }

    /**
     * Return the token, the ServiceAccountToken secrets are automounted.
     * The token file would then be accessible at /var/run/secrets/kubernetes.io/serviceaccount
     * @return the token file content.
     */
    private final static String getAccountToken() {
        InputStream is = null;
        try {
            File file = new File("/var/run/secrets/kubernetes.io/serviceaccount/token");
            final byte[] data = new byte[(int) file.length()];
            is = new FileInputStream(file);
            is.read(data);
            return new String(data);
        } catch (final IOException e) {
            throw new RuntimeException("Could not get token file", e);
        } finally {
            if(is != null) {
                try {
                    is.close();
                } catch (final IOException e) {
                    throw new RuntimeException("Could not close stream and releases any system resources associated with the stream", e);
                }
            }
        }
    }
}
