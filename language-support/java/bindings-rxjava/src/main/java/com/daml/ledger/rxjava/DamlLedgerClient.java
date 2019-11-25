// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.rxjava.grpc.*;
import com.digitalasset.grpc.adapter.SingleThreadExecutionSequencerPool;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link LedgerClient} implementation that connects to
 * an existing Ledger and provides clients to query it. To use the {@link DamlLedgerClient}:
 * <ol>
 *     <li>Create an instance of {@link DamlLedgerClient} using {@link DamlLedgerClient#forLedgerIdAndHost(String, String, int, Optional)},
 *     {@link DamlLedgerClient#forHostWithLedgerIdDiscovery(String, int, Optional)} or {@link DamlLedgerClient#DamlLedgerClient(Optional, ManagedChannel)}</li>
 *     <li>When ready to use the clients, call the method {@link DamlLedgerClient#connect()} to initialize the clients
 *     for that particular Ledger</li>
 *     <li>Retrieve one of the clients by using a getter, e.g. {@link DamlLedgerClient#getActiveContractSetClient()}</li>
 * </ol>
 * For information on how to set up an {@link SslContext} object for mutual authentication please refer to
 * the <a href="https://github.com/grpc/grpc-java/blob/master/SECURITY.md">section on security</a> in the grpc-java documentation.
 *
 */
public final class DamlLedgerClient implements LedgerClient {

    public final static class Builder {

        private final NettyChannelBuilder builder;
        private Optional<String> expectedLedgerId = Optional.empty();

        private Builder(@NonNull NettyChannelBuilder channelBuilder) {
            this.builder = channelBuilder;
            this.builder.usePlaintext();
        }

        public Builder withExpectedLedgerId(@NonNull String expectedLedgerId) {
            this.expectedLedgerId = Optional.of(expectedLedgerId);
            return this;
        }

        public Builder withSslContext(@NonNull SslContext sslContext) {
            this.builder.sslContext(sslContext);
            this.builder.useTransportSecurity();
            return this;
        }

        public DamlLedgerClient build() {
            return new DamlLedgerClient(this.builder, this.expectedLedgerId);
        }

    }

    private static final String DEFAULT_POOL_NAME = "client";

    private final SingleThreadExecutionSequencerPool pool = new SingleThreadExecutionSequencerPool(DEFAULT_POOL_NAME);

    /**
     * Create a new {@link Builder} with the given parameters
     *
     * Useful as a shortcut unless you have to customize the {@link NettyChannelBuilder} beyond the builder's capabilities
     */
    public static Builder newBuilder(@NonNull String host, int port) {
        return new Builder(NettyChannelBuilder.forAddress(host, port));
    }

    /**
     * Create a new {@link Builder} with the given parameters
     *
     * Useful to customize the {@link NettyChannelBuilder} beyond the builder's capabilities,
     * otherwise {@link DamlLedgerClient#newBuilder(String, int)} is probably more suited for your use case
     */
    public static Builder newBuilder(@NonNull NettyChannelBuilder channelBuilder) {
        return new Builder(channelBuilder);
    }

    /**
     * Creates a {@link DamlLedgerClient} connected to a Ledger
     * identified by the ip and port.
     *
     * @param ledgerId The expected ledger-id
     * @param hostIp The ip of the Ledger
     * @param hostPort The port of the Ledger
     * @param sslContext If present, it will be used to establish a TLS connection. If empty, an unsecured plaintext connection will be used.
     *                   Must be an SslContext created for client applications via {@link GrpcSslContexts#forClient()}.
     * @deprecated since 0.13.38, please use {@link DamlLedgerClient#DamlLedgerClient(NettyChannelBuilder, Optional)} or even better either {@link DamlLedgerClient#newBuilder}
     */
    @Deprecated
    public static DamlLedgerClient forLedgerIdAndHost(@NonNull String ledgerId, @NonNull String hostIp, int hostPort, @NonNull Optional<SslContext> sslContext) {
        Builder builder = newBuilder(hostIp, hostPort).withExpectedLedgerId(ledgerId);
        sslContext.ifPresent(builder::withSslContext);
        return builder.build();
    }

    /**
     * Like {@link DamlLedgerClient#forLedgerIdAndHost(String, String, int, Optional)} but with the ledger-id
     * automatically discovered instead of provided.
     * @deprecated since 0.13.38, please use {@link DamlLedgerClient#DamlLedgerClient(NettyChannelBuilder, Optional)} or even better either {@link DamlLedgerClient#newBuilder}
     */
    @Deprecated
    public static DamlLedgerClient forHostWithLedgerIdDiscovery(@NonNull String hostIp, int hostPort, Optional<SslContext> sslContext) {
        Builder builder = newBuilder(hostIp, hostPort);
        sslContext.ifPresent(builder::withSslContext);
        return builder.build();
    }

    private ActiveContractsClient activeContractsClient;
    private TransactionsClient transactionsClient;
    private CommandCompletionClient commandCompletionClient;
    private CommandClient commandClient;
    private CommandSubmissionClient commandSubmissionClient;
    private LedgerIdentityClient ledgerIdentityClient;
    private PackageClient packageClient;
    private LedgerConfigurationClient ledgerConfigurationClient;
    private TimeClient timeClient;
    private String expectedLedgerId;
    private final ManagedChannel channel;

    private DamlLedgerClient(@NonNull NettyChannelBuilder channelBuilder, @NonNull Optional<String> expectedLedgerId) {
        this.channel = channelBuilder.build();
        this.expectedLedgerId = expectedLedgerId.orElse(null);
    }

    /**
     * Creates a {@link DamlLedgerClient} with a previously created {@link ManagedChannel}. This is useful in
     * case additional settings need to be configured for the connection to the ledger (e.g. keep alive timeout).
     * @param expectedLedgerId If the value is present, {@link DamlLedgerClient#connect()} throws an exception
     *                         if the provided ledger id does not match the ledger id provided by the ledger.
     * @param channel A user provided instance of @{@link ManagedChannel}.
     * @deprecated since 0.13.38, please use {@link DamlLedgerClient#newBuilder}
     */
    @Deprecated
    public DamlLedgerClient(Optional<String> expectedLedgerId, @NonNull ManagedChannel channel) {
        this.channel = channel;
        this.expectedLedgerId = expectedLedgerId.orElse(null);
    }

    /**
     * Connects this instance of the {@link DamlLedgerClient} to the Ledger.
     */
    public void connect() {
        ledgerIdentityClient = new LedgerIdentityClientImpl(channel);

        String reportedLedgerId = ledgerIdentityClient.getLedgerIdentity().blockingGet();

        if (this.expectedLedgerId != null && !this.expectedLedgerId.equals(reportedLedgerId)) {
            throw new IllegalArgumentException(String.format("Configured ledger id [%s] is not the same as reported by the ledger [%s]", this.expectedLedgerId, reportedLedgerId));
        } else {
            this.expectedLedgerId = reportedLedgerId;
        }

        activeContractsClient = new ActiveContractClientImpl(reportedLedgerId, channel, pool);
        transactionsClient = new TransactionClientImpl(reportedLedgerId, channel, pool);
        commandCompletionClient = new CommandCompletionClientImpl(reportedLedgerId, channel, pool);
        commandSubmissionClient = new CommandSubmissionClientImpl(reportedLedgerId, channel);
        commandClient = new CommandClientImpl(reportedLedgerId, channel);
        packageClient = new PackageClientImpl(reportedLedgerId, channel);
        ledgerConfigurationClient = new LedgerConfigurationClientImpl(reportedLedgerId, channel, pool);
        timeClient = new TimeClientImpl(reportedLedgerId, channel, pool);
    }


    @Override
    public String getLedgerId() {
        return expectedLedgerId;
    }

    @Override
    public ActiveContractsClient getActiveContractSetClient() {
        return activeContractsClient;
    }

    @Override
    public TransactionsClient getTransactionsClient() {
        return transactionsClient;
    }

    @Override
    public CommandClient getCommandClient() {
        return commandClient;
    }

    @Override
    public CommandCompletionClient getCommandCompletionClient() {
        return commandCompletionClient;
    }

    @Override
    public CommandSubmissionClient getCommandSubmissionClient() {
        return commandSubmissionClient;
    }

    @Override
    public LedgerIdentityClient getLedgerIdentityClient() {
        return ledgerIdentityClient;
    }

    @Override
    public PackageClient getPackageClient() {
        return packageClient;
    }

    @Override
    public LedgerConfigurationClient getLedgerConfigurationClient() {
        return ledgerConfigurationClient;
    }

    @Override
    public TimeClient getTimeClient() { return timeClient; }

    public void close() throws Exception {
        channel.shutdownNow();
        while (!channel.awaitTermination(1, TimeUnit.SECONDS)) {

        }
        pool.close();
    }

    public static void main(String[] args) {
        DamlLedgerClient ledgerClient = DamlLedgerClient.forHostWithLedgerIdDiscovery("localhost", 6865, Optional.empty());
        ledgerClient.connect();
        String ledgerId = ledgerClient.ledgerIdentityClient.getLedgerIdentity().blockingGet();
        System.out.println("expectedLedgerId = " + ledgerId);
        LinkedList<String> packageIds = ledgerClient.packageClient
                .listPackages()
                .collect((Callable<LinkedList<String>>) LinkedList::new, LinkedList::addFirst)
                .blockingGet();
        System.out.println("packageIds = " + packageIds);
        ledgerClient.ledgerConfigurationClient
                .getLedgerConfiguration()
                .take(1)
                .blockingForEach(c -> System.out.println("ledger-configuration " + c));
    }
}
