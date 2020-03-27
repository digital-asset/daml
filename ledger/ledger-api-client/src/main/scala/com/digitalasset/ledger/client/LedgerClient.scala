// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.ledger.client.services.commands.{CommandClient, SynchronousCommandClient}
import com.digitalasset.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.stub.AbstractStub

import scala.concurrent.{ExecutionContext, Future}

final class LedgerClient private (
    val channel: Channel,
    config: LedgerClientConfiguration,
    val ledgerId: LedgerId
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory) {

  val activeContractSetClient =
    new ActiveContractSetClient(
      ledgerId,
      LedgerClient.stub(ActiveContractsServiceGrpc.stub(channel), config.token))

  val commandClient: CommandClient =
    new CommandClient(
      LedgerClient.stub(CommandSubmissionServiceGrpc.stub(channel), config.token),
      LedgerClient.stub(CommandCompletionServiceGrpc.stub(channel), config.token),
      ledgerId,
      config.applicationId,
      config.commandClient
    )

  val commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(LedgerClient.stub(CommandServiceGrpc.stub(channel), config.token))

  val packageClient: PackageClient =
    new PackageClient(ledgerId, LedgerClient.stub(PackageServiceGrpc.stub(channel), config.token))

  val packageManagementClient: PackageManagementClient =
    new PackageManagementClient(
      LedgerClient.stub(PackageManagementServiceGrpc.stub(channel), config.token))

  val partyManagementClient: PartyManagementClient =
    new PartyManagementClient(
      LedgerClient.stub(PartyManagementServiceGrpc.stub(channel), config.token))

  val transactionClient: TransactionClient =
    new TransactionClient(
      ledgerId,
      LedgerClient.stub(TransactionServiceGrpc.stub(channel), config.token))

}

object LedgerClient {

  def apply(
      channel: Channel,
      config: LedgerClientConfiguration
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    for {
      ledgerId <- new LedgerIdentityClient(
        stub(LedgerIdentityServiceGrpc.stub(channel), config.token))
        .satisfies(config.ledgerIdRequirement)
    } yield {
      new LedgerClient(channel, config, ledgerId)
    }
  }

  private[client] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))

  /**
    * A convenient shortcut to build a [[LedgerClient]], use [[fromBuilder]] for a more
    * flexible alternative.
    */
  def singleHost(hostIp: String, port: Int, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] =
    fromBuilder(NettyChannelBuilder.forAddress(hostIp, port), configuration)

  @deprecated("Use the safer and more flexible `fromBuilder` method", "0.13.35")
  def forChannel(configuration: LedgerClientConfiguration, channel: Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] =
    apply(channel, configuration)

  /**
    * Takes a [[NettyChannelBuilder]], possibly set up with some relevant extra options
    * that cannot be specified though the [[LedgerClientConfiguration]] (e.g. a set of
    * default [[io.grpc.CallCredentials]] to be used with all calls unless explicitly
    * set on a per-call basis), sets the relevant options specified by the configuration
    * (possibly overriding the existing builder settings), and returns a [[LedgerClient]].
    *
    * A shutdown hook is also added to close the channel when the JVM stops.
    *
    */
  def fromBuilder(builder: NettyChannelBuilder, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    configuration.sslContext.fold(builder.usePlaintext())(
      builder.sslContext(_).negotiationType(NegotiationType.TLS))
    val channel = builder.build()
    val _ = sys.addShutdownHook { val _ = channel.shutdownNow() }
    apply(channel, configuration)
  }

}
