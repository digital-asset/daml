// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.daml.grpc.AuthCallCredentials.authorizingStub
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc
import com.daml.ledger.api.v2.state_service.StateServiceGrpc
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.daml.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import io.grpc.Channel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.Closeable
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

/** GRPC client for the Canton Ledger API.
  *
  * Tracing support: we use CallOptions, see [[com.digitalasset.canton.tracing.TraceContextGrpc]].
  */
final class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    @unused
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable {

  lazy val commandService = new CommandServiceClient(
    CommandServiceGrpc.stub(channel),
    config.token,
  )

  lazy val packageService = new PackageClient(
    PackageServiceGrpc.stub(channel),
    config.token,
  )

  lazy val stateService = new StateServiceClient(
    StateServiceGrpc.stub(channel),
    config.token,
  )

  lazy val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      PackageManagementServiceGrpc.stub(channel),
      config.token,
    )

  lazy val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      PartyManagementServiceGrpc.stub(channel),
      config.token,
    )

  lazy val userManagementClient: UserManagementClient =
    new UserManagementClient(
      UserManagementServiceGrpc.stub(channel),
      config.token,
    )

  override def close(): Unit = GrpcChannel.close(channel)
}

object LedgerClient {

  def apply(
      channel: Channel,
      config: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      traceContext: TraceContext,
  ): Future[LedgerClient] =
    for {
      // requesting ledger end validates the token, thus guaranteeing that the client is operable
      _ <- new StateServiceClient(
        StateServiceGrpc.stub(channel)
      ).getLedgerEnd(config.token())
    } yield new LedgerClient(channel, config, loggerFactory)

  def withoutToken(
      channel: Channel,
      config: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClient =
    new LedgerClient(channel, config, loggerFactory)

  private[client] def stubWithTracing[A <: AbstractStub[A]](stub: A, token: Option[String])(implicit
      traceContext: TraceContext
  ): A =
    token
      .fold(stub)(authorizingStub(stub, _))
      .withInterceptors(TraceContextGrpc.clientInterceptor())
      .withOption(TraceContextGrpc.TraceContextOptionsKey, traceContext)

  /** A convenient shortcut to build a [[LedgerClient]], use [[fromBuilder]] for a more flexible
    * alternative.
    */
  def singleHost(
      hostIp: String,
      port: Int,
      configuration: LedgerClientConfiguration,
      channelConfig: LedgerClientChannelConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      traceContext: TraceContext,
  ): Future[LedgerClient] =
    fromBuilder(channelConfig.builderFor(hostIp, port), configuration, loggerFactory)

  def insecureSingleHost(
      hostIp: String,
      port: Int,
      configuration: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      traceContext: TraceContext,
  ): Future[LedgerClient] =
    fromBuilder(
      LedgerClientChannelConfiguration.InsecureDefaults.builderFor(hostIp, port),
      configuration,
      loggerFactory,
    )

  /** Takes a [[io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder]], sets the relevant options
    * specified by the configuration (possibly overriding the existing builder settings), and
    * returns a [[LedgerClient]].
    *
    * A shutdown hook is also added to close the channel when the JVM stops.
    */
  def fromBuilder(
      builder: NettyChannelBuilder,
      configuration: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      traceContext: TraceContext,
  ): Future[LedgerClient] =
    LedgerClient(
      GrpcChannel.withShutdownHook(builder),
      configuration,
      loggerFactory,
    )

}
