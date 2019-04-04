// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import com.digitalasset.ledger.client.impl.LedgerClientImpl
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.{CommandClient, SynchronousCommandClient}
import com.digitalasset.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import scala.concurrent.{ExecutionContext, Future}

trait LedgerClient {

  def ledgerId: String

  def activeContractSetClient: ActiveContractSetClient

  def commandClient: CommandClient

  def commandServiceClient: SynchronousCommandClient

  def packageClient: PackageClient

  def transactionClient: TransactionClient

}

object LedgerClient {

  def apply(
      ledgerIdentityService: LedgerIdentityService,
      transactionService: TransactionService,
      activeContractsService: ActiveContractsService,
      commandSubmissionService: CommandSubmissionService,
      commandCompletionService: CommandCompletionService,
      commandService: CommandService,
      packageService: PackageService,
      ledgerClientConfiguration: LedgerClientConfiguration
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory): Future[LedgerClient] = {

    for {
      ledgerId <- new LedgerIdentityClient(ledgerIdentityService)
        .satisfies(ledgerClientConfiguration.ledgerIdRequirement)
    } yield {
      new LedgerClientImpl(
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

  def singleHost(hostIp: String, port: Int, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {

    val builder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(hostIp, port)

    configuration.sslContext
      .fold {
        builder.usePlaintext()
      } { sslContext =>
        builder.sslContext(sslContext).negotiationType(NegotiationType.TLS)
      }

    val channel = builder.build()

    val _ = sys.addShutdownHook { val _ = channel.shutdownNow() }

    forChannel(configuration, channel)

  }
  def forChannel(configuration: LedgerClientConfiguration, channel: ManagedChannel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {
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
}
