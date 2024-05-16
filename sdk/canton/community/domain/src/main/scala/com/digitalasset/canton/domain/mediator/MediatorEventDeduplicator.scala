// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.syntax.alternative.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorDeduplicationStore
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{DynamicDomainParametersWithValidity, RequestId}
import com.digitalasset.canton.sequencing.TracedProtocolEvent
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

private[mediator] trait MediatorEventDeduplicator {

  /** Reads the request uuids of envelopes and checks for duplicates:
    * If the uuid of an envelope has been used previously and the previous usage has not expired by the
    * sequencer timestamp of the corresponding event, then the event is rejected through the sequencer client.
    * If the uuid of an envelope is fresh, it will be stored as being "in use".
    *
    * The method should not be called concurrently.
    * The method may be invoked again, as soon as the future returned by the previous invocation has completed, i.e.,
    * `rejectDuplicates(...).isComplete`.
    *
    * @return `(uniqueEnvelopesByEvent, storeF)`:
    *   `uniqueEnvelopesByEvent` contains those elements of `envelopesByEvent` that have no UUID or a unique UUID.
    *   `storeF` completes when the persistent state has been updated and all rejections have been sent.
    *   The method `rejectDuplicates` may be invoked again while `storeF` is still running;
    *   The event should be considered clean only when `storeF` is completed.
    */
  def rejectDuplicates(
      envelopesByEvent: Seq[(TracedProtocolEvent, Seq[DefaultOpenEnvelope])]
  )(implicit
      executionContext: ExecutionContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[
    (Seq[(TracedProtocolEvent, Seq[DefaultOpenEnvelope])], FutureUnlessShutdown[Unit])
  ] =
    MonadUtil
      .sequentialTraverse(envelopesByEvent) { case (tracedProtocolEvent, envelopes) =>
        implicit val traceContext: TraceContext = tracedProtocolEvent.traceContext
        rejectDuplicates(tracedProtocolEvent.value.timestamp, envelopes)(
          traceContext,
          callerCloseContext,
        ).map { case (uniqueEnvelopes, storeF) =>
          (tracedProtocolEvent, uniqueEnvelopes) -> storeF
        }
      }
      .map(_.separate)
      .map { case (results, storeFs) => results -> storeFs.sequence_ }

  /** See the comment of the other `rejectDuplicates` method.
    * @return `(uniqueEnvelopes, storeF)`, where `uniqueEnvelopes` contains those elements
    *   of `envelopes` that have no UUID or a unique UUID and
    *   `storeF` completes when the persistent state has been updated and all rejections have been sent.
    */
  def rejectDuplicates(
      requestTimestamp: CantonTimestamp,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[(Seq[DefaultOpenEnvelope], FutureUnlessShutdown[Unit])]
}

private[mediator] object MediatorEventDeduplicator {
  def create(
      store: MediatorDeduplicationStore,
      verdictSender: VerdictSender,
      topologyClient: DomainTopologyClient,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): MediatorEventDeduplicator = {

    def getDomainParameters(
        tracedRequestTime: Traced[CantonTimestamp]
    ): Future[DynamicDomainParametersWithValidity] =
      tracedRequestTime.withTraceContext { implicit traceContext => requestTime =>
        for {
          snapshot <- topologyClient.awaitSnapshot(requestTime)
          domainParameters <- snapshot
            .findDynamicDomainParameters()
            .flatMap(_.toFuture(new RuntimeException(_)))
        } yield domainParameters
      }

    def getDeduplicationTimeout(
        tracedRequestTime: Traced[CantonTimestamp]
    ): FutureUnlessShutdown[Duration] = FutureUnlessShutdown.outcomeF(
      getDomainParameters(tracedRequestTime).map(_.mediatorDeduplicationTimeout.duration)
    )

    def getDecisionTime(
        tracedRequestTime: Traced[CantonTimestamp]
    ): FutureUnlessShutdown[CantonTimestamp] = FutureUnlessShutdown.outcomeF(
      getDomainParameters(tracedRequestTime).flatMap(_.decisionTimeForF(tracedRequestTime.value))
    )

    new DefaultMediatorEventDeduplicator(
      store,
      verdictSender,
      getDeduplicationTimeout,
      getDecisionTime,
      protocolVersion,
      loggerFactory,
    )
  }
}

class DefaultMediatorEventDeduplicator(
    store: MediatorDeduplicationStore,
    verdictSender: VerdictSender,
    getDeduplicationTimeout: Traced[CantonTimestamp] => FutureUnlessShutdown[Duration],
    getDecisionTime: Traced[CantonTimestamp] => FutureUnlessShutdown[CantonTimestamp],
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorEventDeduplicator
    with NamedLogging {

  override def rejectDuplicates(
      requestTimestamp: CantonTimestamp,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[(Seq[DefaultOpenEnvelope], FutureUnlessShutdown[Unit])] =
    MonadUtil
      .sequentialTraverse(envelopes) { envelope =>
        envelope.protocolMessage match {
          case request: MediatorConfirmationRequest =>
            processUuid(requestTimestamp, request, envelopes).map { case (hasUniqueUuid, storeF) =>
              Option.when(hasUniqueUuid)(envelope) -> storeF
            }
          case _: ProtocolMessage =>
            FutureUnlessShutdown.pure(Some(envelope) -> FutureUnlessShutdown.unit)
        }
      }
      .map(_.separate)
      .map { case (uniqueEnvelopeOs, storeFs) =>
        (uniqueEnvelopeOs.flattenOption, storeFs.sequence_)
      }

  private def processUuid(
      requestTimestamp: CantonTimestamp,
      request: MediatorConfirmationRequest,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[(Boolean, FutureUnlessShutdown[Unit])] = {
    val uuid = request.requestUuid
    val previousUsages = store.findUuid(uuid, requestTimestamp)
    NonEmpty.from(previousUsages) match {
      case None =>
        for {
          deduplicationTimeout <- getDeduplicationTimeout(Traced(requestTimestamp))
          expireAt = requestTimestamp.plus(deduplicationTimeout)
        } yield {
          logger.debug(
            s"Storing requestUuid=$uuid, requestTimestamp=$requestTimestamp, expireAt=$expireAt"
          )
          val storeF = store.store(uuid, requestTimestamp, expireAt)
          (true, storeF)
        }
      case Some(previousUsagesNE) =>
        val expireAfter = previousUsagesNE.map(_.expireAfter).max1
        val rejection = MediatorError.MalformedMessage.Reject(
          s"The request uuid ($uuid) must not be used until $expireAfter."
        )
        rejection.report()

        val rootHashMessages = envelopes.mapFilter(
          ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
        )

        for {
          decisionTime <- getDecisionTime(Traced(requestTimestamp))
        } yield {
          val sendF =
            verdictSender.sendReject(
              RequestId(requestTimestamp),
              Some(request),
              rootHashMessages,
              MediatorVerdict.MediatorReject(rejection).toVerdict(protocolVersion),
              decisionTime,
            )
          (false, sendF)
        }
    }
  }
}
