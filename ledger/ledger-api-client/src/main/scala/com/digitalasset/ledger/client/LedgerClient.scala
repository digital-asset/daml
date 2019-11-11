// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsServiceStub
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandServiceStub
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceStub
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionServiceStub
import com.digitalasset.ledger.client.auth.LedgerClientCallCredentials.authenticatingStub
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.{CommandClient, SynchronousCommandClient}
import com.digitalasset.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.stub.AbstractStub

import scala.concurrent.{ExecutionContext, Future}

final class LedgerClient(
    transactionService: TransactionServiceStub,
    activeContractsService: ActiveContractsServiceStub,
    commandSubmissionService: CommandSubmissionServiceStub,
    commandCompletionService: CommandCompletionServiceStub,
    commandService: CommandServiceStub,
    packageService: PackageServiceStub,
    ledgerClientConfiguration: LedgerClientConfiguration,
    val ledgerId: LedgerId
)(implicit esf: ExecutionSequencerFactory) {

  val activeContractSetClient =
    new ActiveContractSetClient(ledgerId, activeContractsService)

  val commandClient: CommandClient =
    new CommandClient(
      commandSubmissionService,
      commandCompletionService,
      ledgerId,
      ledgerClientConfiguration.applicationId,
      ledgerClientConfiguration.commandClient
    )

  val commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(commandService)

  val packageClient: PackageClient =
    new PackageClient(ledgerId, packageService)

  val transactionClient: TransactionClient =
    new TransactionClient(ledgerId, transactionService)

}

object LedgerClient {

  def apply(
      ledgerIdentityService: LedgerIdentityServiceStub,
      transactionService: TransactionServiceStub,
      activeContractsService: ActiveContractsServiceStub,
      commandSubmissionService: CommandSubmissionServiceStub,
      commandCompletionService: CommandCompletionServiceStub,
      commandService: CommandServiceStub,
      packageService: PackageServiceStub,
      ledgerClientConfiguration: LedgerClientConfiguration
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    for {
      ledgerId <- new LedgerIdentityClient(ledgerIdentityService)
        .satisfies(ledgerClientConfiguration.ledgerIdRequirement)
    } yield {
      new LedgerClient(
        transactionService,
        activeContractsService,
        commandSubmissionService,
        commandCompletionService,
        commandService,
        packageService,
        ledgerClientConfiguration,
        ledgerId
      )
    }
  }

  private[client] def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub))

  /**
    * Constructs a [[Channel]], ensuring that the [[LedgerClientConfiguration]] is picked up and valid
    *
    * You'll generally want to use [[singleHost]], use this only if you need a higher level of control
    * over your [[Channel]].
    */
  def constructChannel(
      hostIp: String,
      port: Int,
      configuration: LedgerClientConfiguration): NettyChannelBuilder = {
    val builder: NettyChannelBuilder = NettyChannelBuilder.forAddress(hostIp, port)
    configuration.sslContext.fold(builder.usePlaintext())(
      builder.sslContext(_).negotiationType(NegotiationType.TLS))
    builder
  }

  /**
    * A convenient shortcut to build a [[LedgerClient]]
    */
  def singleHost(hostIp: String, port: Int, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    val channel = constructChannel(hostIp, port, configuration).build
    val _ = sys.addShutdownHook { val _ = channel.shutdownNow() }
    forChannel(configuration, channel)
  }

  def forChannel(configuration: LedgerClientConfiguration, channel: Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] =
    apply(
      LedgerIdentityServiceGrpc.stub(channel),
      TransactionServiceGrpc.stub(channel),
      ActiveContractsServiceGrpc.stub(channel),
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      CommandServiceGrpc.stub(channel),
      PackageServiceGrpc.stub(channel),
      configuration
    )

}
