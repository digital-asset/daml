// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.withoutledgerid

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.event_query_service.EventQueryServiceGrpc
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.digitalasset.canton.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.ledger.client.services.EventQueryServiceClient
import com.digitalasset.canton.ledger.client.services.acs.withoutledgerid.ActiveContractSetClient
import com.digitalasset.canton.ledger.client.services.admin.{
  MeteringReportClient,
  PackageManagementClient,
  PartyManagementClient,
  UserManagementClient,
}
import com.digitalasset.canton.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.canton.ledger.client.services.commands.withoutledgerid.CommandClient
import com.digitalasset.canton.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.canton.ledger.client.services.pkg.withoutledgerid.PackageClient
import com.digitalasset.canton.ledger.client.services.transactions.withoutledgerid.TransactionClient
import com.digitalasset.canton.ledger.client.services.version.withoutledgerid.VersionClient
import com.digitalasset.canton.ledger.client.{GrpcChannel, LedgerClient as ClassicLedgerClient}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder

import java.io.Closeable
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable
    with NamedLogging {

  val activeContractSetClient =
    new ActiveContractSetClient(
      ClassicLedgerClient.stub(ActiveContractsServiceGrpc.stub(channel), config.token)
    )

  val commandClient: CommandClient =
    new CommandClient(
      ClassicLedgerClient.stub(CommandSubmissionServiceGrpc.stub(channel), config.token),
      ClassicLedgerClient.stub(CommandCompletionServiceGrpc.stub(channel), config.token),
      config.applicationId,
      config.commandClient,
      loggerFactory,
    )

  val commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(
      ClassicLedgerClient.stub(CommandServiceGrpc.stub(channel), config.token)
    )

  val packageClient: PackageClient =
    new PackageClient(
      ClassicLedgerClient.stub(PackageServiceGrpc.stub(channel), config.token)
    )

  val meteringReportClient: MeteringReportClient =
    new MeteringReportClient(
      ClassicLedgerClient.stub(MeteringReportServiceGrpc.stub(channel), config.token)
    )

  val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      ClassicLedgerClient.stub(PackageManagementServiceGrpc.stub(channel), config.token)
    )

  val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      ClassicLedgerClient.stub(PartyManagementServiceGrpc.stub(channel), config.token)
    )

  val transactionClient: TransactionClient =
    new TransactionClient(
      ClassicLedgerClient.stub(TransactionServiceGrpc.stub(channel), config.token)
    )

  val versionClient: VersionClient =
    new VersionClient(
      ClassicLedgerClient.stub(VersionServiceGrpc.stub(channel), config.token)
    )

  val userManagementClient: UserManagementClient = new UserManagementClient(
    ClassicLedgerClient.stub(UserManagementServiceGrpc.stub(channel), config.token)
  )

  val identityClient =
    new LedgerIdentityClient(
      ClassicLedgerClient.stub(
        LedgerIdentityServiceGrpc.stub(channel): @nowarn(
          "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
        ),
        config.token,
      )
    )

  val eventQueryServiceClient = new EventQueryServiceClient(
    ClassicLedgerClient.stub(EventQueryServiceGrpc.stub(channel), config.token)
  )

  override def close(): Unit = GrpcChannel.close(channel)
}

object LedgerClient {
  def apply(
      channel: Channel,
      config: LedgerClientConfiguration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClient =
    new LedgerClient(channel, config, loggerFactory)

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
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): LedgerClient =
    LedgerClient(GrpcChannel.withShutdownHook(builder), configuration, loggerFactory)

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
  ): LedgerClient =
    fromBuilder(channelConfig.builderFor(hostIp, port), configuration, loggerFactory)

}
