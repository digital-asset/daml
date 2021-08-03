// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.grpc.adapter.SingleThreadExecutionSequencerPool;
import com.daml.ledger.rxjava.grpc.*;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A {@link LedgerClient} implementation that connects to an existing Ledger and provides clients to
 * query it. To use the {@link DamlLedgerClient}:
 *
 * <ol>
 *   <li>Create an instance of a {@link Builder} using {@link DamlLedgerClient#newBuilder(String,
 *       int)}
 *   <li>Specify an expected ledger identifier, {@link SslContext}, and/or access token, depending
 *       on your needs
 *   <li>Invoke {@link Builder#build()} to finalize and construct a {@link DamlLedgerClient}
 *   <li>Call the method {@link DamlLedgerClient#connect()} to initialize the clients for that
 *       particular ledger
 *   <li>Retrieve one of the clients by using a getter, e.g. {@link
 *       DamlLedgerClient#getActiveContractSetClient()}
 * </ol>
 *
 * <p>Alternatively to {@link DamlLedgerClient#newBuilder(String, int)}, you can use {@link
 * DamlLedgerClient#newBuilder(NettyChannelBuilder)} to make sure you can specify additional
 * properties for the channel you're building, such as the maximum inbound message size.
 *
 * <p>For information on how to set up an {@link SslContext} object for mutual authentication please
 * refer to the <a href="https://github.com/grpc/grpc-java/blob/master/SECURITY.md">section on
 * security</a> in the grpc-java documentation.
 */
public final class DamlLedgerClient implements LedgerClient {

  public static final class Builder {

    private final NettyChannelBuilder builder;
    private Optional<String> expectedLedgerId = Optional.empty();
    private Optional<String> accessToken = Optional.empty();
    private Optional<Deadline> deadline = Optional.empty();

    private Builder(@NonNull NettyChannelBuilder channelBuilder) {
      this.builder = channelBuilder;
      this.builder.usePlaintext();
    }

    public Builder withSslContext(@NonNull SslContext sslContext) {
      this.builder.sslContext(sslContext);
      this.builder.useTransportSecurity();
      return this;
    }

    public Builder withExpectedLedgerId(@NonNull String expectedLedgerId) {
      this.expectedLedgerId = Optional.of(expectedLedgerId);
      return this;
    }

    public Builder withAccessToken(@NonNull String accessToken) {
      this.accessToken = Optional.of(accessToken);
      return this;
    }

    public Builder withDeadline(@NonNull Deadline deadline) {
      this.deadline = Optional.of(deadline);
      return this;
    }

    public DamlLedgerClient build() {
      return new DamlLedgerClient(
          this.builder, this.expectedLedgerId, this.accessToken, this.deadline);
    }
  }

  private static final String DEFAULT_POOL_NAME = "client";

  private final SingleThreadExecutionSequencerPool pool =
      new SingleThreadExecutionSequencerPool(DEFAULT_POOL_NAME);

  /**
   * Create a new {@link Builder} with the given parameters
   *
   * <p>Useful as a shortcut unless you have to customize the {@link NettyChannelBuilder} beyond the
   * builder's capabilities
   */
  public static Builder newBuilder(@NonNull String host, int port) {
    return new Builder(NettyChannelBuilder.forAddress(host, port));
  }

  /**
   * Create a new {@link Builder} with the given parameters
   *
   * <p>Useful to customize the {@link NettyChannelBuilder} beyond the builder's capabilities,
   * otherwise {@link DamlLedgerClient#newBuilder(String, int)} is probably more suited for your use
   * case
   */
  public static Builder newBuilder(@NonNull NettyChannelBuilder channelBuilder) {
    return new Builder(channelBuilder);
  }

  /**
   * Creates a {@link DamlLedgerClient} connected to a Ledger identified by the ip and port.
   *
   * @param ledgerId The expected ledger-id
   * @param hostIp The ip of the Ledger
   * @param hostPort The port of the Ledger
   * @param sslContext If present, it will be used to establish a TLS connection. If empty, an
   *     unsecured plaintext connection will be used. Must be an SslContext created for client
   *     applications via {@link GrpcSslContexts#forClient()}.
   * @deprecated since 0.13.38, please use {@link
   *     DamlLedgerClient#DamlLedgerClient(NettyChannelBuilder, Optional, Optional, Optional)} or
   *     even better either {@link DamlLedgerClient#newBuilder}
   */
  @Deprecated
  public static DamlLedgerClient forLedgerIdAndHost(
      @NonNull String ledgerId,
      @NonNull String hostIp,
      int hostPort,
      @NonNull Optional<SslContext> sslContext) {
    Builder builder = newBuilder(hostIp, hostPort).withExpectedLedgerId(ledgerId);
    sslContext.ifPresent(builder::withSslContext);
    return builder.build();
  }

  /**
   * Like {@link DamlLedgerClient#forLedgerIdAndHost(String, String, int, Optional)} but with the
   * ledger-id automatically discovered instead of provided.
   *
   * @deprecated since 0.13.38, please use {@link
   *     DamlLedgerClient#DamlLedgerClient(NettyChannelBuilder, Optional, Optional, Optional)} or
   *     even better either {@link DamlLedgerClient#newBuilder}
   */
  @Deprecated
  public static DamlLedgerClient forHostWithLedgerIdDiscovery(
      @NonNull String hostIp, int hostPort, Optional<SslContext> sslContext) {
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
  private Optional<String> accessToken;
  private final Optional<Deadline> deadline;
  private final ManagedChannel channel;

  private DamlLedgerClient(
      @NonNull NettyChannelBuilder channelBuilder,
      @NonNull Optional<String> expectedLedgerId,
      @NonNull Optional<String> accessToken,
      @NonNull Optional<Deadline> deadline) {
    this.channel = channelBuilder.build();
    this.expectedLedgerId = expectedLedgerId.orElse(null);
    this.accessToken = accessToken;
    this.deadline = deadline;
  }

  /**
   * Creates a {@link DamlLedgerClient} with a previously created {@link ManagedChannel}. This is
   * useful in case additional settings need to be configured for the connection to the ledger (e.g.
   * keep alive timeout).
   *
   * @param expectedLedgerId If the value is present, {@link DamlLedgerClient#connect()} throws an
   *     exception if the provided ledger id does not match the ledger id provided by the ledger.
   * @param channel A user provided instance of @{@link ManagedChannel}.
   * @deprecated since 0.13.38, please use {@link DamlLedgerClient#newBuilder}
   */
  @Deprecated
  public DamlLedgerClient(
      Optional<String> expectedLedgerId,
      @NonNull ManagedChannel channel,
      Optional<Deadline> deadline) {
    this.channel = channel;
    this.expectedLedgerId = expectedLedgerId.orElse(null);
    this.accessToken = Optional.empty();
    this.deadline = deadline;
  }

  /** Connects this instance of the {@link DamlLedgerClient} to the Ledger. */
  public void connect() {
    ledgerIdentityClient = new LedgerIdentityClientImpl(channel, this.accessToken);

    String reportedLedgerId = ledgerIdentityClient.getLedgerIdentity().blockingGet();

    if (this.expectedLedgerId != null && !this.expectedLedgerId.equals(reportedLedgerId)) {
      throw new IllegalArgumentException(
          String.format(
              "Configured ledger id [%s] is not the same as reported by the ledger [%s]",
              this.expectedLedgerId, reportedLedgerId));
    } else {
      this.expectedLedgerId = reportedLedgerId;
    }

    activeContractsClient =
        new ActiveContractClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    transactionsClient =
        new TransactionClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    commandCompletionClient =
        new CommandCompletionClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    commandSubmissionClient =
        new CommandSubmissionClientImpl(reportedLedgerId, channel, this.accessToken, this.deadline);
    commandClient = new CommandClientImpl(reportedLedgerId, channel, this.accessToken);
    packageClient = new PackageClientImpl(reportedLedgerId, channel, this.accessToken);
    ledgerConfigurationClient =
        new LedgerConfigurationClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    timeClient = new TimeClientImpl(reportedLedgerId, channel, pool, this.accessToken);
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
  public TimeClient getTimeClient() {
    return timeClient;
  }

  public void close() throws Exception {
    channel.shutdownNow();
    channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    pool.close();
  }
}
