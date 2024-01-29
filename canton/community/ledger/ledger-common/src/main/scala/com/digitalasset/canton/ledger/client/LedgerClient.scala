// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v1.trace_context.TraceContext as LedgerApiTraceContext
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc as CommandServiceGrpcV2
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc as PackageServiceGrpcV2
import com.daml.ledger.api.v2.state_service.StateServiceGrpc
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc
import com.digitalasset.canton.ledger.client.LedgerCallCredentials.authenticatingStub
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
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.Closeable
import scala.concurrent.{ExecutionContext, Future}

/** GRPC client for the Canton Ledger API.
  *
  * Tracing support:
  *   In order to propagate the TraceContext through the API, just run
  *     traceContext.context.makeCurrent()
  *     before any request invocation.
  */
final class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable {

  object v2 {
    lazy val commandService = new CommandServiceClient(
      LedgerClient.stub(CommandServiceGrpcV2.stub(channel), config.token)
    )
    lazy val eventQueryService = new EventQueryServiceClient(
      LedgerClient.stub(EventQueryServiceGrpc.stub(channel), config.token)
    )
    lazy val packageService = new PackageClient(
      LedgerClient.stub(PackageServiceGrpcV2.stub(channel), config.token)
    )

    lazy val stateService = new StateServiceClient(
      LedgerClient.stub(StateServiceGrpc.stub(channel), config.token)
    )

    lazy val updateService = new UpdateServiceClient(
      LedgerClient.stub(UpdateServiceGrpc.stub(channel), config.token)
    )

    lazy val versionClient: VersionClient =
      new VersionClient(LedgerClient.stub(VersionServiceGrpc.stub(channel), config.token))
  }

  lazy val identityProviderConfigClient: IdentityProviderConfigClient =
    new IdentityProviderConfigClient(
      LedgerClient.stub(IdentityProviderConfigServiceGrpc.stub(channel), config.token)
    )

  lazy val meteringReportClient: MeteringReportClient =
    new MeteringReportClient(
      LedgerClient.stub(MeteringReportServiceGrpc.stub(channel), config.token)
    )

  lazy val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      LedgerClient.stub(PackageManagementServiceGrpc.stub(channel), config.token)
    )

  lazy val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      LedgerClient.stub(PartyManagementServiceGrpc.stub(channel), config.token)
    )

  lazy val userManagementClient: UserManagementClient =
    new UserManagementClient(
      LedgerClient.stub(UserManagementServiceGrpc.stub(channel), config.token)
    )

  lazy val participantPruningManagementClient: ParticipantPruningManagementClient =
    new ParticipantPruningManagementClient(
      LedgerClient.stub(ParticipantPruningServiceGrpc.stub(channel), config.token)
    )

  override def close(): Unit = GrpcChannel.close(channel)
}

object LedgerClient {

  def apply(
      channel: Channel,
      config: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] =
    for {
      // requesting ledger end validates the token, thus guaranteeing that the client is operable
      _ <- new StateServiceClient(
        StateServiceGrpc.stub(channel)
      ).getLedgerEnd(config.token)
    } yield new LedgerClient(channel, config, loggerFactory)

  def withoutToken(
      channel: Channel,
      config: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClient =
    new LedgerClient(channel, config, loggerFactory)

  private[client] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub, _))

  /** A convenient shortcut to build a [[LedgerClient]], use [[fromBuilder]] for a more
    * flexible alternative.
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
  ): Future[LedgerClient] =
    fromBuilder(
      LedgerClientChannelConfiguration.InsecureDefaults.builderFor(hostIp, port),
      configuration,
      loggerFactory,
    )

  /** Takes a [[io.grpc.netty.NettyChannelBuilder]], possibly set up with some relevant extra options
    * that cannot be specified though the [[com.digitalasset.canton.ledger.client.configuration.LedgerClientConfiguration]] (e.g. a set of
    * default [[io.grpc.CallCredentials]] to be used with all calls unless explicitly
    * set on a per-call basis), sets the relevant options specified by the configuration
    * (possibly overriding the existing builder settings), and returns a [[LedgerClient]].
    *
    * A shutdown hook is also added to close the channel when the JVM stops.
    */
  def fromBuilder(
      builder: NettyChannelBuilder,
      configuration: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    LedgerClient(
      GrpcChannel.withShutdownHook(builder),
      configuration,
      loggerFactory,
    )
  }

  /** Extract a trace context from a transaction and represent it as our TraceContext */
  def traceContextFromLedgerApi(traceContext: Option[LedgerApiTraceContext]): TraceContext =
    traceContext match {
      case Some(LedgerApiTraceContext(Some(parent), state)) =>
        W3CTraceContext(parent, state).toTraceContext
      case _ => TraceContext.withNewTraceContext(identity)
    }

}
