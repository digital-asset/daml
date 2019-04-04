// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pipeline.samples.helloworldtls;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServerTls} with TLS.
 */
final class HelloWorldClientTls {

    final private GreeterGrpc.GreeterBlockingStub blockingStub;

    private static SslContext getSslContext(PrivateKey privateKey, X509Certificate trustCertCollection, X509Certificate clientCertChain) throws SSLException {
        return GrpcSslContexts.forClient().keyManager(privateKey, clientCertChain).trustManager(trustCertCollection).build();
    }

    public HelloWorldClientTls(String host, int port, PrivateKey privateKey, X509Certificate certChain, X509Certificate trustCertCollection) throws SSLException {
        SslContext sslContext = getSslContext(privateKey, trustCertCollection, certChain);
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port).negotiationType(NegotiationType.TLS).sslContext(sslContext).build();
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public String sayHello(String toWhom) {
        return blockingStub.sayHello(HelloRequest.newBuilder().setName(toWhom).build()).getMessage();
    }

}
