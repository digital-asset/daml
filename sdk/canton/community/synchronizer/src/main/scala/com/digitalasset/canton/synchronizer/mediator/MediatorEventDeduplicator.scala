// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{DynamicSynchronizerParametersWithValidity, RequestId}
import com.digitalasset.canton.synchronizer.mediator.store.MediatorState
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration
import scala.concurrent.ExecutionContext

private[mediator] trait MediatorEventDeduplicator {

  /** If the uuid of the confirmation request has been used previously and the previous usage has
    * not expired by the sequencer timestamp of the corresponding event, then the event is rejected
    * through the sequencer client. If the uuid of an envelope is fresh, it will be stored as being
    * "in use".
    *
    * @return
    *   `(isUnique, storeF)`: `isUnique` is `true`, if the confirmation request was considered to be
    *   unique and its UUID doesn't clash with previously processed confirmation requests.
    *   Conversely, it is `false` if the confirmation request is considered to be a duplicate
    *   submission. `storeF` completes when the persistent state has been updated and all rejections
    *   have been sent. The method `rejectDuplicate` may be invoked again while `storeF` is still
    *   running; The event should be considered clean only when `storeF` is completed.
    */
  def rejectDuplicate(
      requestTimestamp: CantonTimestamp,
      mediatorConfirmationRequest: MediatorConfirmationRequest,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[(Boolean, FutureUnlessShutdown[Unit])]
}

private[mediator] object MediatorEventDeduplicator {
  def create(
      state: MediatorState,
      verdictSender: VerdictSender,
      topologyClient: SynchronizerTopologyClient,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): MediatorEventDeduplicator = {

    def getSynchronizerParameters(
        tracedRequestTime: Traced[CantonTimestamp]
    ): FutureUnlessShutdown[DynamicSynchronizerParametersWithValidity] =
      tracedRequestTime.withTraceContext { implicit traceContext => requestTime =>
        for {
          snapshot <- topologyClient.awaitSnapshot(requestTime)
          synchronizerParameters <-
            snapshot
              .findDynamicSynchronizerParameters()
              .flatMap(_.toFutureUS(new RuntimeException(_)))
        } yield synchronizerParameters
      }

    def getDeduplicationTimeout(
        tracedRequestTime: Traced[CantonTimestamp]
    ): FutureUnlessShutdown[Duration] =
      getSynchronizerParameters(tracedRequestTime).map(_.mediatorDeduplicationTimeout.duration)

    def getDecisionTime(
        tracedRequestTime: Traced[CantonTimestamp]
    ): FutureUnlessShutdown[CantonTimestamp] =
      getSynchronizerParameters(tracedRequestTime).flatMap(
        _.decisionTimeForF(tracedRequestTime.value)
      )

    new DefaultMediatorEventDeduplicator(
      state,
      verdictSender,
      getDeduplicationTimeout,
      getDecisionTime,
      protocolVersion,
      loggerFactory,
    )
  }
}

class DefaultMediatorEventDeduplicator(
    state: MediatorState,
    verdictSender: VerdictSender,
    getDeduplicationTimeout: Traced[CantonTimestamp] => FutureUnlessShutdown[Duration],
    getDecisionTime: Traced[CantonTimestamp] => FutureUnlessShutdown[CantonTimestamp],
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorEventDeduplicator
    with NamedLogging {

  override def rejectDuplicate(
      requestTimestamp: CantonTimestamp,
      confirmationRequest: MediatorConfirmationRequest,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      callerCloseoContext: CloseContext,
  ): FutureUnlessShutdown[(Boolean, FutureUnlessShutdown[Unit])] = {
    val duplicateUUID = findDuplicatesInStore(requestTimestamp, confirmationRequest)
    duplicateUUID match {
      case Some(expireAfter) =>
        // In case we found a duplicate request, reject it and don't process any other envelopes downstream.
        finalizeRejection(requestTimestamp, confirmationRequest, expireAfter, envelopes)
          .map(false -> _)
      case None =>
        processUniqueRequest(requestTimestamp, confirmationRequest).map(true -> _)
    }
  }

  /** Checks whether the confirmation requests is a duplicate.
    * @return
    *   the timestamp after which the UUID can be used again
    */
  private def findDuplicatesInStore(
      requestTimestamp: CantonTimestamp,
      request: MediatorConfirmationRequest,
  )(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] =
    state.deduplicationStore
      .findUuid(request.requestUuid, requestTimestamp)
      .map(_.expireAfter)
      .maxOption

  /** Stores the finalized verdict (reject) on the synchronous path and sends the verdict
    * asynchronously.
    */
  private def finalizeRejection(
      requestTimestamp: CantonTimestamp,
      request: MediatorConfirmationRequest,
      expireAfter: CantonTimestamp,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[FutureUnlessShutdown[Unit]] = {
    val rejection = MediatorError.MalformedMessage.Reject(
      s"The request uuid (${request.requestUuid}) must not be used until $expireAfter."
    )
    rejection.report()

    val requestId = RequestId(requestTimestamp)
    val verdict = MediatorVerdict.MediatorReject(rejection).toVerdict(protocolVersion)
    val finalizedResponse = FinalizedResponse(requestId, request, requestTimestamp, verdict)(
      traceContext
    )
    val rootHashMessages = envelopes.mapFilter(
      ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
    )
    for {
      _ <- state.add(finalizedResponse)
    } yield {
      val sendF = for {
        decisionTime <- getDecisionTime(Traced(requestTimestamp))
        _ <- verdictSender.sendReject(
          requestId,
          Some(request),
          rootHashMessages,
          verdict,
          decisionTime,
        )
      } yield ()
      sendF
    }
  }

  /** Stores deduplication data for the confirmation request. */
  private def processUniqueRequest(
      requestTimestamp: CantonTimestamp,
      request: MediatorConfirmationRequest,
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[FutureUnlessShutdown[Unit]] =
    for {
      deduplicationTimeout <- getDeduplicationTimeout(Traced(requestTimestamp))
      expireAt = requestTimestamp.plus(deduplicationTimeout)
    } yield {
      logger.debug(
        s"Storing requestUuid=${request.requestUuid}, requestTimestamp=$requestTimestamp, expireAt=$expireAt"
      )
      val storeF =
        state.deduplicationStore.store(request.requestUuid, requestTimestamp, expireAt)
      storeF
    }
}
