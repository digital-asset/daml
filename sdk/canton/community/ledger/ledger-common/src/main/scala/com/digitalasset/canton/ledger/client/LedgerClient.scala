// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.grpc.AuthCallCredentials.authorizingStub
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc as CommandServiceGrpcV2
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc as PackageServiceGrpcV2
import com.daml.ledger.api.v2.state_service.StateServiceGrpc
import com.daml.ledger.api.v2.trace_context.TraceContext as LedgerApiTraceContext
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc
import com.digitalasset.canton.ledger.client.LedgerClient.stubWithTracing
import com.digitalasset.canton.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.ledger.client.services.EventQueryServiceClient
import com.digitalasset.canton.ledger.client.services.admin.*
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc, W3CTraceContext}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.Closeable
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

/** GRPC client for the Canton Ledger API.
  *
  * Tracing support: we use CallOptions, see [[com.digitalasset.canton.tracing.TraceContextGrpc]]
  */
final class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    @unused
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable {

  lazy val commandService = new CommandServiceClient(
    CommandServiceGrpcV2.stub(channel),
    config.token,
  )
  lazy val eventQueryService = new EventQueryServiceClient(
    EventQueryServiceGrpc.stub(channel),
    config.token,
  )
  lazy val packageService = new PackageClient(
    PackageServiceGrpcV2.stub(channel),
    config.token,
  )

  lazy val stateService = new StateServiceClient(
    StateServiceGrpc.stub(channel),
    config.token,
  )

  lazy val updateService = new UpdateServiceClient(
    UpdateServiceGrpc.stub(channel),
    config.token,
  )

  lazy val versionClient: VersionClient =
    new VersionClient(VersionServiceGrpc.stub(channel), config.token)

  lazy val identityProviderConfigClient: IdentityProviderConfigClient =
    new IdentityProviderConfigClient(
      IdentityProviderConfigServiceGrpc.stub(channel),
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

  lazy val participantPruningManagementClient: ParticipantPruningManagementClient =
    new ParticipantPruningManagementClient(
      ParticipantPruningServiceGrpc.stub(channel),
      config.token,
    )

  override def close(): Unit = GrpcChannel.close(channel)

  def serviceClient[A <: AbstractStub[A]](stub: Channel => A, token: Option[String])(implicit
      traceContext: TraceContext
  ): A =
    stubWithTracing(stub(channel), token)
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

  private[client] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(
      authorizingStub(stub, _).withInterceptors(TraceContextGrpc.clientInterceptor)
    )

  private[client] def stubWithTracing[A <: AbstractStub[A]](stub: A, token: Option[String])(implicit
      traceContext: TraceContext
  ): A =
    token
      .fold(stub)(authorizingStub(stub, _))
      .withInterceptors(TraceContextGrpc.clientInterceptor)
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

  /** Takes a [[io.grpc.netty.NettyChannelBuilder]], possibly set up with some relevant extra
    * options that cannot be specified though the
    * [[com.digitalasset.canton.ledger.client.configuration.LedgerClientConfiguration]] (e.g. a set
    * of default [[io.grpc.CallCredentials]] to be used with all calls unless explicitly set on a
    * per-call basis), sets the relevant options specified by the configuration (possibly overriding
    * the existing builder settings), and returns a [[LedgerClient]].
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

  /** Extract a trace context from a transaction and represent it as our TraceContext */
  def traceContextFromLedgerApi(traceContext: Option[LedgerApiTraceContext]): TraceContext =
    traceContext match {
      case Some(LedgerApiTraceContext(Some(parent), state)) =>
        W3CTraceContext(parent, state).toTraceContext
      case _ => TraceContext.withNewTraceContext("ledger_api")(identity)
    }

}
