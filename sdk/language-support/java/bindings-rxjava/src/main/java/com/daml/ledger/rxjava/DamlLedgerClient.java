// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.grpc.adapter.SingleThreadExecutionSequencerPool;
import com.daml.ledger.rxjava.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import java.time.Duration;
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
    private Optional<Duration> timeout = Optional.empty();

    public static final int DefaultMaxInboundMessageSize = 10 * 1024 * 1024;

    private Builder(@NonNull NettyChannelBuilder channelBuilder) {
      this.builder = channelBuilder;
      this.builder.usePlaintext();
    }

    public Builder withSslContext(@NonNull SslContext sslContext) {
      this.builder.sslContext(sslContext);
      this.builder.useTransportSecurity();
      return this;
    }

    /**
     * @deprecated since 2.0 the ledger identifier has been deprecated as a fail-safe against
     *     contacting an unexpected participant. You are recommended to use the participant
     *     identifier in the access token as a way to validate that you are accessing the ledger
     *     through the expected participant node (as well as authorize your calls).
     */
    @Deprecated
    public Builder withExpectedLedgerId(@NonNull String expectedLedgerId) {
      this.expectedLedgerId = Optional.of(expectedLedgerId);
      return this;
    }

    public Builder withAccessToken(@NonNull String accessToken) {
      this.accessToken = Optional.of(accessToken);
      return this;
    }

    public Builder withTimeout(@NonNull Duration timeout) {
      this.timeout = Optional.of(timeout);
      return this;
    }

    public Builder withMaxInboundMessageSize(int maxInboundMessageSize) {
      this.builder.maxInboundMessageSize(maxInboundMessageSize);
      return this;
    }

    public DamlLedgerClient build() {
      return new DamlLedgerClient(
          this.builder, this.expectedLedgerId, this.accessToken, this.timeout);
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
    return new Builder(
        NettyChannelBuilder.forAddress(host, port)
            .maxInboundMessageSize(Builder.DefaultMaxInboundMessageSize));
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

  private ActiveContractsClient activeContractsClient;
  private TransactionsClient transactionsClient;
  private CommandCompletionClient commandCompletionClient;
  private CommandClient commandClient;
  private CommandSubmissionClient commandSubmissionClient;
  @Deprecated private LedgerIdentityClient ledgerIdentityClient;
  private PackageClient packageClient;
  private LedgerConfigurationClient ledgerConfigurationClient;
  private TimeClient timeClient;
  private UserManagementClient userManagementClient;
  private String expectedLedgerId;
  private Optional<String> accessToken;
  private final Optional<Duration> timeout;
  private final ManagedChannel channel;

  private DamlLedgerClient(
      @NonNull NettyChannelBuilder channelBuilder,
      @NonNull Optional<String> expectedLedgerId,
      @NonNull Optional<String> accessToken,
      @NonNull Optional<Duration> timeout) {
    this.channel = channelBuilder.build();
    this.expectedLedgerId = expectedLedgerId.orElse(null);
    this.accessToken = accessToken;
    this.timeout = timeout;
  }

  /** Connects this instance of the {@link DamlLedgerClient} to the Ledger. */
  public void connect() {
    @SuppressWarnings("deprecation")
    var lic = new LedgerIdentityClientImpl(channel, this.accessToken, this.timeout);
    ledgerIdentityClient = lic;

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
        new CommandSubmissionClientImpl(reportedLedgerId, channel, this.accessToken, this.timeout);
    commandClient = new CommandClientImpl(reportedLedgerId, channel, this.accessToken);
    packageClient = new PackageClientImpl(reportedLedgerId, channel, this.accessToken);
    ledgerConfigurationClient =
        new LedgerConfigurationClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    timeClient = new TimeClientImpl(reportedLedgerId, channel, pool, this.accessToken);
    userManagementClient = new UserManagementClientImpl(channel, this.accessToken);
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

  @Deprecated
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

  @Override
  public UserManagementClient getUserManagementClient() {
    return userManagementClient;
  }

  public void close() throws Exception {
    channel.shutdownNow();
    channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    pool.close();
  }
}
