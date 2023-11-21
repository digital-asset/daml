// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.digitalasset.canton.ledger.api.auth.client.LedgerCallCredentials.authenticatingStub
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.canton.ledger.client.services.admin.*
import com.digitalasset.canton.ledger.client.services.commands.{
  CommandClient,
  SynchronousCommandClient,
}
import com.digitalasset.canton.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.ledger.client.services.transactions.TransactionClient
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

import java.io.Closeable
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    val ledgerId: LedgerId,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory)
    extends Closeable {

  val activeContractSetClient =
    new ActiveContractSetClient(
      ledgerId,
      LedgerClient.stub(ActiveContractsServiceGrpc.stub(channel), config.token),
    )

  val commandClient: CommandClient =
    new CommandClient(
      LedgerClient.stub(CommandSubmissionServiceGrpc.stub(channel), config.token),
      LedgerClient.stub(CommandCompletionServiceGrpc.stub(channel), config.token),
      ledgerId,
      config.applicationId,
      config.commandClient,
      loggerFactory,
    )

  val commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(LedgerClient.stub(CommandServiceGrpc.stub(channel), config.token))

  val identityProviderConfigClient: IdentityProviderConfigClient =
    new IdentityProviderConfigClient(
      LedgerClient.stub(IdentityProviderConfigServiceGrpc.stub(channel), config.token)
    )

  val packageClient: PackageClient =
    new PackageClient(ledgerId, LedgerClient.stub(PackageServiceGrpc.stub(channel), config.token))

  val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      LedgerClient.stub(PackageManagementServiceGrpc.stub(channel), config.token)
    )

  val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      LedgerClient.stub(PartyManagementServiceGrpc.stub(channel), config.token)
    )

  val transactionClient: TransactionClient =
    new TransactionClient(
      ledgerId,
      LedgerClient.stub(TransactionServiceGrpc.stub(channel), config.token),
    )

  val versionClient: VersionClient =
    new VersionClient(ledgerId, LedgerClient.stub(VersionServiceGrpc.stub(channel), config.token))

  val userManagementClient: UserManagementClient =
    new UserManagementClient(
      LedgerClient.stub(UserManagementServiceGrpc.stub(channel), config.token)
    )

  val participantPruningManagementClient: ParticipantPruningManagementClient =
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
      ledgerId <- new LedgerIdentityClient(
        LedgerClient.stub(
          LedgerIdentityServiceGrpc.stub(channel): @nowarn(
            "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
          ),
          config.token,
        )
      ).satisfies(config.ledgerIdRequirement)
    } yield new LedgerClient(channel, config, ledgerId, loggerFactory)

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
}
