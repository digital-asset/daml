// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.Duration
import java.util.concurrent.TimeUnit.MINUTES

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, LedgerId, Party}
import com.daml.ledger.api.refinements.{CompositeCommand, CompositeCommandAdapter}
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.daml.ledger.client.binding.retrying.CommandRetryFlow
import com.daml.ledger.client.binding.util.Slf4JLogger
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.util.Ctx
import io.grpc.ManagedChannel
import io.grpc.netty.NegotiationType.TLS
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.SslContext
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

class LedgerClientBinding(
    val ledgerClient: LedgerClient,
    val ledgerClientConfig: LedgerClientConfiguration,
    val channel: ManagedChannel,
    retryTimeout: Duration,
    timeProvider: TimeProvider,
    decoder: DecoderType,
) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  import LedgerClientBinding._

  def transactionSource(
      party: Party,
      templateSelector: TemplateSelector,
      startOffset: LedgerOffset,
      endOffset: Option[LedgerOffset],
      token: Option[String] = None,
  ): Source[DomainTransaction, NotUsed] = {

    logger.debug(
      "[tx {}] subscription start with offset template selector {}, start {}, end {}",
      Party.unwrap(party),
      templateSelector,
      startOffset,
      endOffset,
    )

    ledgerClient.transactionClient
      .getTransactions(
        startOffset,
        endOffset,
        transactionFilter(party, templateSelector),
        token = token,
      )
      .via(
        Slf4JLogger(
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
        )
      )
      .via(DomainTransactionMapper(decoder))
  }

  type CommandTrackingFlow[C] =
    Flow[Ctx[C, CompositeCommand], Ctx[C, Either[CompletionFailure, CompletionSuccess]], NotUsed]

  private val compositeCommandAdapter = new CompositeCommandAdapter(
    LedgerId(ledgerClient.ledgerId.unwrap),
    ApplicationId(ledgerClientConfig.applicationId),
  )

  def retryingConfirmedCommands[C](
      party: Party
  )(implicit ec: ExecutionContext): Future[CommandTrackingFlow[C]] =
    for {
      tracking <- CommandRetryFlow[C](
        party,
        ledgerClient.commandClient,
        timeProvider,
        retryTimeout,
      )
    } yield Flow[Ctx[C, CompositeCommand]]
      .map(
        _.map(compositeCommand =>
          CommandSubmission(compositeCommandAdapter.transform(compositeCommand))
        )
      )
      .via(tracking)

  type CommandsFlow[C] =
    Flow[Ctx[C, CompositeCommand], Ctx[C, Either[CompletionFailure, CompletionSuccess]], NotUsed]

  def commands[C](party: Party)(implicit ec: ExecutionContext): Future[CommandsFlow[C]] = {
    for {
      trackCommandsFlow <- ledgerClient.commandClient
        .trackCommands[C](List(party.unwrap), token = None)
    } yield Flow[Ctx[C, CompositeCommand]]
      .map(
        _.map(compositeCommand =>
          CommandSubmission(compositeCommandAdapter.transform(compositeCommand))
        )
      )
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

  @nowarn(
    "msg=parameter config .* is never used"
  ) // public function, unsure whether arg needed
  def askLedgerId(
      channel: ManagedChannel,
      config: LedgerClientConfiguration,
  )(implicit ec: ExecutionContext): Future[String] =
    LedgerIdentityServiceGrpc
      .stub(channel)
      .getLedgerIdentity(GetLedgerIdentityRequest())
      .map(_.ledgerId): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

  def transactionFilter(party: Party, templateSelector: TemplateSelector): TransactionFilter =
    TransactionFilter(Map(party.unwrap -> templateSelector.toApi))

}
