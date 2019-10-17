// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client

import java.util.concurrent.Executor

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
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
import io.grpc.{CallCredentials, Channel, Metadata}
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import scala.concurrent.{ExecutionContext, Future}

trait LedgerClient {

  def ledgerId: LedgerId

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

  /**
    * Constructs a [[Channel]], ensuring that the [[LedgerClientConfiguration]] is picked up and valid
    *
    * You'll generally want to use [[singleHost]], use this only if you need a higher level of control
    * over your [[Channel]].
    */
  def constructChannel(
      hostIp: String,
      port: Int,
      configuration: LedgerClientConfiguration): Channel = {
    val builder: NettyChannelBuilder = NettyChannelBuilder.forAddress(hostIp, port)
    configuration.sslContext.fold(builder.usePlaintext())(
      builder.sslContext(_).negotiationType(NegotiationType.TLS))
    val channel = builder.build()
    val _ = sys.addShutdownHook { val _ = channel.shutdownNow() }
    channel
  }

  /**
    * A convenient shortcut to build a [[LedgerClient]]
    */
  def singleHost(hostIp: String, port: Int, configuration: LedgerClientConfiguration)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] =
    forChannel(configuration, constructChannel(hostIp, port, configuration))

  private[this] val auth = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  def callCredentials(token: String): CallCredentials =
    new CallCredentials {
      override def applyRequestMetadata(
          requestInfo: CallCredentials.RequestInfo,
          appExecutor: Executor,
          applier: CallCredentials.MetadataApplier): Unit = {
        val metadata = new Metadata
        metadata.put(auth, s"Bearer $token")
        applier.apply(metadata)
      }

      // Should be a noop but never called; tries to make it clearer to implementors that they may break in the future.
      override def thisUsesUnstableApi(): Unit = ()
    }

  def forChannel(configuration: LedgerClientConfiguration, channel: Channel)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory): Future[LedgerClient] = {
    val creds = configuration.accessToken.map(callCredentials).orNull
    apply(
      LedgerIdentityServiceGrpc.stub(channel).withCallCredentials(creds),
      TransactionServiceGrpc.stub(channel).withCallCredentials(creds),
      ActiveContractsServiceGrpc.stub(channel).withCallCredentials(creds),
      CommandSubmissionServiceGrpc.stub(channel).withCallCredentials(creds),
      CommandCompletionServiceGrpc.stub(channel).withCallCredentials(creds),
      CommandServiceGrpc.stub(channel).withCallCredentials(creds),
      PackageServiceGrpc.stub(channel).withCallCredentials(creds),
      configuration
    )
  }
}
