// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessSet
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponse,
  ProtocolMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.client.{SendCallback, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId, Recipients}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import scala.concurrent.{ExecutionContext, Future}

/** Collects helper methods for message processing */
abstract class AbstractMessageProcessor(
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClientSend,
    protocolVersion: ProtocolVersion,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  protected def terminateRequest(
      requestCounter: RequestCounter,
      requestSequencerCounter: SequencerCounter,
      requestTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
      eventO: Option[Traced[Update]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- ephemeral.requestJournal.terminate(
        requestCounter,
        requestTimestamp,
        commitTime,
      )
      _ <- ephemeral.recordOrderPublisher.tick(
        requestSequencerCounter,
        requestTimestamp,
        eventO,
        Some(requestCounter),
      )
    } yield ()

  /** A clean replay replays a request whose request counter is below the clean head in the request journal.
    * Since the replayed request is clean, its effects are not persisted.
    */
  protected def isCleanReplay(requestCounter: RequestCounter): Boolean =
    requestCounter < ephemeral.startingPoints.processing.nextRequestCounter

  protected def unlessCleanReplay(requestCounter: RequestCounter)(f: => Future[_]): Future[Unit] =
    if (isCleanReplay(requestCounter)) Future.unit else f.void

  protected def signResponse(ips: DomainSnapshotSyncCryptoApi, response: ConfirmationResponse)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[SignedProtocolMessage[ConfirmationResponse]] =
    SignedProtocolMessage.trySignAndCreate(response, ips, protocolVersion)

  // Assumes that we are not closing (i.e., that this is synchronized with shutdown somewhere higher up the call stack)
  protected def sendResponses(
      requestId: RequestId,
      messages: Seq[(ProtocolMessage, Recipients)],
      // use client.messageId. passed in here such that we can log it before sending
      messageId: Option[MessageId] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    implicit val metricsContext: MetricsContext = MetricsContext(
      "type" -> "send-confirmation-response"
    )
    if (messages.isEmpty) FutureUnlessShutdown.unit
    else {
      logger.trace(s"Request $requestId: ProtocolProcessor scheduling the sending of responses")
      for {
        domainParameters <- crypto.ips
          .awaitSnapshotUS(requestId.unwrap)
          .flatMap(snapshot =>
            FutureUnlessShutdown.outcomeF(
              snapshot.findDynamicDomainParametersOrDefault(protocolVersion)
            )
          )

        maxSequencingTime = requestId.unwrap.add(
          domainParameters.confirmationResponseTimeout.unwrap
        )
        _ <- sequencerClient
          .sendAsync(
            Batch.of(protocolVersion, messages*),
            topologyTimestamp = Some(requestId.unwrap),
            maxSequencingTime = maxSequencingTime,
            messageId = messageId.getOrElse(MessageId.randomMessageId()),
            callback = SendCallback.log(s"Response message for request [$requestId]", logger),
            amplify = true,
          )
          .valueOr {
            // Swallow Left errors to avoid stopping request processing, as sending response could fail for arbitrary reasons
            // if the sequencer rejects them (e.g max sequencing time has elapsed)
            err =>
              logger.warn(s"Request $requestId: Failed to send responses: ${err.show}")
          }
      } yield ()
    }
  }

  /** Immediately moves the request to Confirmed and
    * register a timeout handler at the decision time with the request tracker
    * to cover the case that the mediator does not send a confirmation result.
    */
  protected def prepareForMediatorResultOfBadRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    crypto.ips
      .awaitSnapshotUS(timestamp)
      .flatMap(snapshot => FutureUnlessShutdown.outcomeF(snapshot.findDynamicDomainParameters()))
      .flatMap { domainParametersE =>
        val decisionTimeE = domainParametersE.flatMap(_.decisionTimeFor(timestamp))
        val decisionTimeF = decisionTimeE.fold(
          err => Future.failed(new IllegalStateException(err)),
          Future.successful,
        )

        def onTimeout: Future[Unit] = {
          logger.debug(
            s"Bad request $requestCounter: Timed out without a confirmation result message."
          )
          performUnlessClosingF(functionFullName) {

            decisionTimeF.flatMap(
              terminateRequest(requestCounter, sequencerCounter, timestamp, _, None)
            )

          }.onShutdown {
            logger.info(s"Ignoring timeout of bad request $requestCounter due to shutdown")
          }
        }

        registerRequestWithTimeout(
          requestCounter,
          sequencerCounter,
          timestamp,
          decisionTimeF,
          onTimeout,
        )
      }

  private def registerRequestWithTimeout(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      decisionTimeF: Future[CantonTimestamp],
      onTimeout: => Future[Unit],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      decisionTime <- FutureUnlessShutdown.outcomeF(decisionTimeF)
      requestFutures <- ephemeral.requestTracker
        .addRequest(
          requestCounter,
          sequencerCounter,
          timestamp,
          timestamp,
          decisionTime,
          ActivenessSet.empty,
        )
        .valueOr(error =>
          ErrorUtil.internalError(new IllegalStateException(show"Request already exists: $error"))
        )
      _ <- FutureUnlessShutdown.outcomeF(
        unlessCleanReplay(requestCounter)(
          ephemeral.requestJournal.insert(requestCounter, timestamp)
        )
      )
      _ <- requestFutures.activenessResult

      _ =
        if (!isCleanReplay(requestCounter)) {
          val timeoutF =
            requestFutures.timeoutResult.flatMap { timeoutResult =>
              if (timeoutResult.timedOut) FutureUnlessShutdown.outcomeF(onTimeout)
              else FutureUnlessShutdown.unit
            }
          FutureUtil.doNotAwaitUnlessShutdown(timeoutF, "Handling timeout failed")
        }
    } yield ()

  /** Transition the request to Clean without doing anything */
  protected def invalidRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      eventO: Option[Traced[Update]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // Let the request immediately timeout (upon the next message) rather than explicitly adding an empty commit set
    // because we don't have a sequencer counter to associate the commit set with.
    val decisionTime = timestamp.immediateSuccessor
    registerRequestWithTimeout(
      requestCounter,
      sequencerCounter,
      timestamp,
      Future.successful(decisionTime),
      terminateRequest(requestCounter, sequencerCounter, timestamp, decisionTime, eventO),
    )
  }
}
