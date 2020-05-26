// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.Duration
import java.util.concurrent.TimeUnit.MINUTES

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, LedgerId, Party}
import com.daml.ledger.api.refinements.{CompositeCommand, CompositeCommandAdapter}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.daml.ledger.client.binding.retrying.{CommandRetryFlow, RetryInfo}
import com.daml.ledger.client.binding.util.Slf4JLogger
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.util.Ctx
import io.grpc.ManagedChannel
import io.grpc.netty.NegotiationType.TLS
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.SslContext
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

class LedgerClientBinding(
    val ledgerClient: LedgerClient,
    val ledgerClientConfig: LedgerClientConfiguration,
    val channel: ManagedChannel,
    retryTimeout: Duration,
    timeProvider: TimeProvider,
    decoder: DecoderType) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  import LedgerClientBinding._

  def transactionSource(
      party: Party,
      templateSelector: TemplateSelector,
      startOffset: LedgerOffset,
      endOffset: Option[LedgerOffset]): Source[DomainTransaction, NotUsed] = {

    logger.debug(
      "[tx {}] subscription start with offset template selector {}, start {}, end {}",
      party,
      templateSelector,
      startOffset,
      endOffset)

    ledgerClient.transactionClient
      .getTransactions(startOffset, endOffset, transactionFilter(party, templateSelector))
      .via(Slf4JLogger(
        logger,
        s"tx $party",
        tx =>
          s"CID ${tx.commandId} TX ${tx.transactionId} CONTAINS ${tx.events
            .map {
              case Event(Event.Event.Created(value)) => s"C ${value.contractId}"
              case Event(Event.Event.Archived(value)) => s"A ${value.contractId}"
              case other => sys.error(s"Expected Created or Archived, got $other"): String
            }
            .mkString("[", ",", "]")}",
        false
      ))
      .via(DomainTransactionMapper(decoder))
  }

  type CommandTrackingFlow[C] = Flow[Ctx[C, CompositeCommand], Ctx[C, Completion], NotUsed]

  private val compositeCommandAdapter = new CompositeCommandAdapter(
    LedgerId(ledgerClient.ledgerId.unwrap),
    ApplicationId(ledgerClientConfig.applicationId),
  )

  def retryingConfirmedCommands[C](party: Party)(
      implicit ec: ExecutionContext): Future[CommandTrackingFlow[C]] =
    for {
      tracking <- CommandRetryFlow[C](
        party,
        ledgerClient.commandClient,
        timeProvider,
        retryTimeout,
        createRetry)
    } yield
      Flow[Ctx[C, CompositeCommand]]
        .map(_.map(compositeCommandAdapter.transform))
        .via(tracking)

  private def createRetry[C](retryInfo: RetryInfo[C], completion: Completion): SubmitRequest = {
    if (retryInfo.request.commands.isEmpty) {
      logger.warn(s"Retrying with empty commands for {}", retryInfo.request)
    }

    retryInfo.request
  }

  type CommandsFlow[C] = Flow[Ctx[C, CompositeCommand], Ctx[C, Completion], NotUsed]

  def commands[C](party: Party)(implicit ec: ExecutionContext): Future[CommandsFlow[C]] = {
    for {
      trackCommandsFlow <- ledgerClient.commandClient.trackCommands[C](List(party.unwrap))
    } yield
      Flow[Ctx[C, CompositeCommand]]
        .map(_.map(compositeCommandAdapter.transform))
        .via(trackCommandsFlow)
        .mapMaterializedValue(_ => NotUsed)
  }

  def shutdown()(implicit ec: ExecutionContext): Future[Unit] = Future {
    channel.shutdown()
    channel.awaitTermination(1, MINUTES)
    ()
  }

}

object LedgerClientBinding {

  def createChannel(host: String, port: Int, sslContext: Option[SslContext]): ManagedChannel = {
    val builder = NettyChannelBuilder.forAddress(host, port)

    sslContext match {
      case Some(context) => builder.sslContext(context).negotiationType(TLS)
      case None => builder.usePlaintext()
    }

    builder.build()
  }

  def askLedgerId(channel: ManagedChannel, config: LedgerClientConfiguration)(
      implicit ec: ExecutionContext): Future[String] =
    LedgerIdentityServiceGrpc
      .stub(channel)
      .getLedgerIdentity(GetLedgerIdentityRequest())
      .map(_.ledgerId)

  def transactionFilter(party: Party, templateSelector: TemplateSelector) =
    TransactionFilter(Map(party.unwrap -> templateSelector.toApi))

}
