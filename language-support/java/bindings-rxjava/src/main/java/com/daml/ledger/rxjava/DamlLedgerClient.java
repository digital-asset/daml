// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
 *       DamlLedgerClient#getStateClient()}
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
    private Optional<String> accessToken = Optional.empty();
    private Optional<Duration> timeout = Optional.empty();

    private Builder(@NonNull NettyChannelBuilder channelBuilder) {
      this.builder = channelBuilder;
      this.builder.usePlaintext();
    }

    public Builder withSslContext(@NonNull SslContext sslContext) {
      this.builder.sslContext(sslContext);
      this.builder.useTransportSecurity();
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

    public DamlLedgerClient build() {
      return new DamlLedgerClient(this.builder, this.accessToken, this.timeout);
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

  private StateClient stateServiceClient;
  private UpdateClient transactionsClient;
  private CommandCompletionClient commandCompletionClient;
  private CommandClient commandClient;
  private CommandSubmissionClient commandSubmissionClient;
  private EventQueryClient eventQueryClient;
  private PackageClient packageClient;
  private TimeClient timeClient;
  private UserManagementClient userManagementClient;
  private Optional<String> accessToken;
  private final Optional<Duration> timeout;
  private final ManagedChannel channel;

  private DamlLedgerClient(
      @NonNull NettyChannelBuilder channelBuilder,
      @NonNull Optional<String> accessToken,
      @NonNull Optional<Duration> timeout) {
    this.channel = channelBuilder.build();
    this.accessToken = accessToken;
    this.timeout = timeout;
  }

  /** Connects this instance of the {@link DamlLedgerClient} to the Ledger. */
  public void connect() {
    stateServiceClient = new StateClientImpl(channel, pool, this.accessToken);
    transactionsClient = new UpdateClientImpl(channel, pool, this.accessToken);
    commandCompletionClient = new CommandCompletionClientImpl(channel, pool, this.accessToken);
    commandSubmissionClient =
        new CommandSubmissionClientImpl(channel, this.accessToken, this.timeout);
    commandClient = new CommandClientImpl(channel, this.accessToken);
    eventQueryClient = new EventQueryClientImpl(channel, this.accessToken);
    packageClient = new PackageClientImpl(channel, this.accessToken);
    timeClient = new TimeClientImpl(channel, pool, this.accessToken);
    userManagementClient = new UserManagementClientImpl(channel, this.accessToken);
  }

  @Override
  public StateClient getStateClient() {
    return stateServiceClient;
  }

  @Override
  public UpdateClient getTransactionsClient() {
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
  public EventQueryClient getEventQueryClient() {
    return eventQueryClient;
  }

  @Override
  public PackageClient getPackageClient() {
    return packageClient;
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
