// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pipeline.samples.helloworldtls;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.function.IntConsumer;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server with TLS enabled.
 */
final class HelloWorldServerTls {

    private final String host;
    private final SslContextBuilder sslContextBuilder;

    public HelloWorldServerTls(String host, PrivateKey privateKey, X509Certificate certChain, X509Certificate trustCertCollection) {
        this.host = host;
        this.sslContextBuilder =
                GrpcSslContexts.configure(
                        SslContextBuilder.
                                forServer(privateKey, certChain).
                                trustManager(trustCertCollection).
                                clientAuth(ClientAuth.REQUIRE),
                        SslProvider.OPENSSL);
    }

    public final void managed(IntConsumer withPort) throws InterruptedException {
        Server server = null;
        try {
            server = NettyServerBuilder.forAddress(new InetSocketAddress(host, 0))
                    .addService(new GreeterImpl())
                    .sslContext(this.sslContextBuilder.build())
                    .build();
            server.start();
            withPort.accept(server.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (server != null) {
                server.shutdown();
                server.awaitTermination();
            }
        }
    }

    static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

    }

}
