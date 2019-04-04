// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pipeline.samples.helloworldtls;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.net.ssl.SSLException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public final class HelloWorldTlsTest {

    static private final String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // Let's try to fall back to a sensible default
            return "0.0.0.0";
        }
    }

    static private final String name = "Brian Kernighan";

    static private final String serverHostName = hostname();

    @Test
    public void tlsWithMutualAuthenticationShouldWork() throws Exception {
        final AuthenticationBundle auth = new AuthenticationBundle(serverHostName, serverHostName, serverHostName);
        HelloWorldServerTls server = new HelloWorldServerTls(serverHostName, auth.serverPrivateKey, auth.serverCertChain, auth.trustCertCollection);
        server.managed(port -> {
            try {
                final HelloWorldClientTls client = new HelloWorldClientTls(serverHostName, port, auth.clientPrivateKey, auth.clientCertChain, auth.trustCertCollection);
                final String reply = client.sayHello(name);
                assertTrue(reply.endsWith(name));
            } catch (SSLException e) {
                fail(e.getMessage());
            }
        });
    }

}
