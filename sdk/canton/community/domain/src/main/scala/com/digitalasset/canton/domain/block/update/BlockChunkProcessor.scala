// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.implicits.catsStdInstancesForFuture
import cats.syntax.alternative.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.LedgerBlockEvent
import com.digitalasset.canton.domain.block.LedgerBlockEvent.{Acknowledgment, Send}
import com.digitalasset.canton.domain.block.update.BlockUpdateGeneratorImpl.{
  SequencedSubmission,
  State,
}
import com.digitalasset.canton.domain.block.update.SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Processes a chunk of events in a block, yielding a [[ChunkUpdate]].
  */
private[update] final class BlockChunkProcessor(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    memberValidator: SequencerMemberValidator,
    createTopologyTickMessageId: () => MessageId = () => MessageId.randomMessageId(), // For testing
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  private val sequencedSubmissionsValidator =
    new SequencedSubmissionsValidator(
      domainId,
      protocolVersion,
      domainSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      loggerFactory,
      metrics,
      memberValidator = memberValidator,
    )

  def processDataChunk(
      state: BlockUpdateGeneratorImpl.State,
      height: Long,
      index: Int,
      chunkEvents: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(BlockUpdateGeneratorImpl.State, ChunkUpdate)] = {
    val (lastTsBeforeValidation, fixedTsChanges) = fixTimestamps(height, index, state, chunkEvents)

    // TODO(i18438): verify the signature of the sequencer on the SendEvent
    val orderingRequests =
      fixedTsChanges.collect { case (ts, ev @ Traced(sendEvent: Send)) =>
        // Discard the timestamp of the `Send` event as we're using the adjusted timestamp
        (ts, ev.map(_ => sendEvent.signedOrderingRequest))
      }

    FutureUtil.doNotAwait(
      recordSubmissionMetrics(fixedTsChanges.map(_._2)),
      "submission metric updating failed",
    )

    for {
      sequencedSubmissionsWithSnapshots <-
        addSnapshots(
          state.latestSequencerEventTimestamp,
          None,
          orderingRequests,
        )

      acksValidationResult <- processAcknowledgements(state, fixedTsChanges)
      (acksByMember, invalidAcks) = acksValidationResult

      validationResult <-
        sequencedSubmissionsValidator.validateSequencedSubmissions(
          state,
          height,
          sequencedSubmissionsWithSnapshots,
        )
      SequencedSubmissionsValidationResult(
        finalInFlightAggregations,
        inFlightAggregationUpdates,
        lastSequencerEventTimestamp,
        reversedOutcomes,
      ) = validationResult

      finalInFlightAggregationsWithAggregationExpiry =
        finalInFlightAggregations.filterNot { case (_, inFlightAggregation) =>
          inFlightAggregation.expired(lastTsBeforeValidation)
        }
      chunkUpdate =
        ChunkUpdate(
          acksByMember,
          invalidAcks,
          inFlightAggregationUpdates,
          lastSequencerEventTimestamp,
          finalInFlightAggregationsWithAggregationExpiry,
          reversedOutcomes.reverse,
        )

      // We don't want to take into consideration events that have possibly been discarded, otherwise we could
      // assign a last ts value to the block based on an event that wasn't included in the block which would cause
      // validations to fail down the line. That's why we need to compute it using only validated events, instead of
      // using the lastTs computed initially pre-validation.
      lastChunkTsOfSuccessfulEvents =
        reversedOutcomes
          .collect { case SubmissionRequestOutcome(_, _, o: DeliverableSubmissionOutcome) =>
            o.sequencingTime
          }
          .maxOption
          .getOrElse(state.lastChunkTs)

      newState =
        BlockUpdateGeneratorImpl.State(
          state.lastBlockTs,
          lastChunkTsOfSuccessfulEvents,
          lastSequencerEventTimestamp.orElse(state.latestSequencerEventTimestamp),
          finalInFlightAggregationsWithAggregationExpiry,
        )
    } yield (newState, chunkUpdate)
  }

  def emitTick(
      state: BlockUpdateGeneratorImpl.State,
      height: Long,
      tickAtLeastAt: CantonTimestamp,
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[(State, ChunkUpdate)] = {
    // The block orderer requests a topology tick to advance the topology processor's time knowledge
    //  whenever it assesses that it may need to retrieve an up-to-date topology snapshot at a certain
    //  sequencing timestamp, and it does so by setting it as in a `RawLedgerBlock`, promising that
    //  all requests at up to (and possibly including) that sequencing timestamp have been already
    //  ordered and included in a block.
    //  That timestamp is `tickAtLeastAt`, which is the earliest timestamp at which the tick
    //  should be sequenced, so that the block orderer's topology snapshot query succeeds.
    //  The last sequenced timestamp before that, i.e. `state.lastChunkTs`, could be either earlier than
    //  or equal to `tickAtLeastAt`: it will be earlier if some requests failed validation and were dropped,
    //  else it may be equal to it, as `tickAtLeastAt` may be, at earliest, exactly the sequencing time of the
    //  last ordered request.
    //  In the latter case, the topology tick should be sequenced at the immediate successor of
    //  `state.lastChunkTs = tickAtLeastAt` because there is already a request sequenced at that
    //  timestamp and sequencing time must be strictly monotonically increasing.
    //  We choose thus the latest between `state.lastChunkTs.immediateSuccessor` and `tickAtLeastAt`.
    //  We also require that the block orderer will not order any other request at `tickSequencingTimestamp`
    //  (see `RawLedgerBlock` for more information).
    val tickSequencingTimestamp = state.lastChunkTs.immediateSuccessor.max(tickAtLeastAt)

    // TODO(#21662) Optimization: if the latest sequencer event timestamp is the same as the last chunk's final
    //  timestamp, then the last chunk's event was sequencer-addressed (and it passed validation),
    //  so it's safe for the block orderer to query the topology snapshot on its sequencing timestamp,
    //  and we don't need to add a `Deliver` for the tick.

    logger.debug(
      s"Block $height: emitting a topology tick at least at $tickAtLeastAt (actually at $tickSequencingTimestamp) " +
        s"as requested by the block orderer"
    )
    // We bypass validation here to make sure that the topology tick is always received by the sequencer runtime.
    for {
      snapshot <-
        SyncCryptoClient.getSnapshotForTimestampUS(
          domainSyncCryptoApi,
          tickSequencingTimestamp,
          state.latestSequencerEventTimestamp,
          protocolVersion,
          warnIfApproximate = false,
        )
      sequencerRecipients <-
        FutureUnlessShutdown.outcomeF(
          GroupAddressResolver.resolveGroupsToMembers(
            Set(SequencersOfDomain),
            snapshot.ipsSnapshot,
          )
        )
    } yield {
      val newState =
        state.copy(
          lastChunkTs = tickSequencingTimestamp,
          latestSequencerEventTimestamp = Some(tickSequencingTimestamp),
        )
      val tickSubmissionOutcome =
        SubmissionRequestOutcome(
          Map.empty, // Sequenced events are legacy and will be removed, so no need to generate them
          None,
          outcome = SubmissionOutcome.Deliver(
            SubmissionRequest.tryCreate(
              sender = sequencerId,
              messageId = createTopologyTickMessageId(),
              batch = Batch.empty(protocolVersion),
              maxSequencingTime = tickSequencingTimestamp,
              topologyTimestamp = None,
              aggregationRule = None,
              submissionCost = None,
              protocolVersion = protocolVersion,
            ),
            sequencingTime = tickSequencingTimestamp,
            deliverToMembers = sequencerRecipients(SequencersOfDomain),
            batch = Batch.empty(protocolVersion),
            submissionTraceContext = TraceContext.createNew(),
            trafficReceiptO = None,
          ),
        )
      val chunkUpdate = ChunkUpdate(
        acknowledgements = Map.empty,
        invalidAcknowledgements = Seq.empty,
        inFlightAggregationUpdates = Map.empty,
        lastSequencerEventTimestamp = Some(tickSequencingTimestamp),
        inFlightAggregations = state.inFlightAggregations,
        submissionsOutcomes = Seq(tickSubmissionOutcome),
      )

      (newState, chunkUpdate)
    }
  }

  private def fixTimestamps(
      height: Long,
      index: Int,
      state: State,
      chunk: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  ): (CantonTimestamp, Seq[(CantonTimestamp, Traced[LedgerBlockEvent])]) = {
    val (lastTsBeforeValidation, revFixedTsChanges) =
      // With this logic, we assign to the initial non-Send events the same timestamp as for the last
      // block. This means that we will include these events in the ephemeral state of the previous block
      // when we re-read it from the database. But this doesn't matter given that all those events are idempotent.
      chunk.forgetNE.foldLeft[
        (CantonTimestamp, Seq[(CantonTimestamp, Traced[LedgerBlockEvent])]),
      ]((state.lastChunkTs, Seq.empty)) { case ((lastTs, events), event) =>
        event.value match {
          case send: Send =>
            val ts = ensureStrictlyIncreasingTimestamp(lastTs, send.timestamp)
            logger.info(
              show"Observed Send with messageId ${send.signedSubmissionRequest.content.messageId.singleQuoted} in block $height, chunk $index and assigned it timestamp $ts"
            )(event.traceContext)
            (ts, (ts, event) +: events)
          case _ =>
            logger.info(
              s"Observed ${event.value} in block $height, chunk $index at timestamp $lastTs"
            )(
              event.traceContext
            )
            (lastTs, (lastTs, event) +: events)
        }
      }
    val fixedTsChanges: Seq[(CantonTimestamp, Traced[LedgerBlockEvent])] = revFixedTsChanges.reverse
    (lastTsBeforeValidation, fixedTsChanges)
  }

  // only accept the provided timestamp if it's strictly greater than the last timestamp
  // otherwise just offset the last valid timestamp by 1
  private def ensureStrictlyIncreasingTimestamp(
      lastTs: CantonTimestamp,
      providedTimestamp: CantonTimestamp,
  ): CantonTimestamp = {
    val invariant = providedTimestamp > lastTs
    orderingTimeFixMode match {

      case OrderingTimeFixMode.ValidateOnly =>
        if (!invariant)
          sys.error(
            "BUG: sequencing timestamps are not strictly monotonically increasing," +
              s" last timestamp $lastTs, provided timestamp: $providedTimestamp"
          )
        providedTimestamp

      case OrderingTimeFixMode.MakeStrictlyIncreasing =>
        if (invariant) {
          providedTimestamp
        } else {
          lastTs.immediateSuccessor
        }
    }
  }

  private def addSnapshots(
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      sequencersSequencerCounter: Option[SequencerCounter],
      submissionRequests: Seq[(CantonTimestamp, Traced[SignedOrderingRequest])],
  )(implicit executionContext: ExecutionContext): FutureUnlessShutdown[Seq[SequencedSubmission]] =
    submissionRequests.parTraverse { case (sequencingTimestamp, tracedSubmissionRequest) =>
      tracedSubmissionRequest.withTraceContext { implicit traceContext => orderingRequest =>
        // Warn if we use an approximate snapshot but only after we've read at least one
        val warnIfApproximate = sequencersSequencerCounter.exists(_ > SequencerCounter.Genesis)
        for {
          topologySnapshotOrErrO <- orderingRequest.submissionRequest.topologyTimestamp.traverse(
            topologyTimestamp =>
              SequencedEventValidator
                .validateTopologyTimestampUS(
                  domainSyncCryptoApi,
                  topologyTimestamp,
                  sequencingTimestamp,
                  latestSequencerEventTimestamp,
                  protocolVersion,
                  warnIfApproximate,
                  _.sequencerTopologyTimestampTolerance,
                )
                .leftMap {
                  case SequencedEventValidator.TopologyTimestampAfterSequencingTime =>
                    SequencerErrors.TopologyTimestampAfterSequencingTimestamp(
                      topologyTimestamp,
                      sequencingTimestamp,
                    )
                  case SequencedEventValidator.TopologyTimestampTooOld(_) |
                      SequencedEventValidator.NoDynamicDomainParameters(_) =>
                    SequencerErrors.TopoologyTimestampTooEarly(
                      topologyTimestamp,
                      sequencingTimestamp,
                    )
                }
                .value
          )
          topologyOrSequencingSnapshot <- topologySnapshotOrErrO match {
            case Some(Right(topologySnapshot)) => FutureUnlessShutdown.pure(topologySnapshot)
            case _ =>
              SyncCryptoClient.getSnapshotForTimestampUS(
                domainSyncCryptoApi,
                sequencingTimestamp,
                latestSequencerEventTimestamp,
                protocolVersion,
                warnIfApproximate,
              )
          }
        } yield SequencedSubmission(
          sequencingTimestamp,
          orderingRequest,
          topologyOrSequencingSnapshot,
          topologySnapshotOrErrO.mapFilter(_.swap.toOption),
        )(traceContext)
      }
    }

  private def processAcknowledgements(
      state: State,
      fixedTsChanges: Seq[(CantonTimestamp, Traced[LedgerBlockEvent])],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    (Map[Member, CantonTimestamp], Seq[(Member, CantonTimestamp, BaseAlarm)])
  ] =
    for {
      snapshot <- SyncCryptoClient.getSnapshotForTimestampUS(
        domainSyncCryptoApi,
        state.lastBlockTs,
        state.latestSequencerEventTimestamp,
        protocolVersion,
        warnIfApproximate = false,
      )
      allAcknowledgements = fixedTsChanges.collect { case (_, t @ Traced(Acknowledgment(ack))) =>
        t.map(_ => ack)
      }
      (goodTsAcks, futureAcks) = allAcknowledgements.partition { tracedSignedAck =>
        // Intentionally use the previous block's last timestamp
        // such that the criterion does not depend on how the block events are chunked up.
        tracedSignedAck.value.content.timestamp <= state.lastBlockTs
      }
      invalidTsAcks = futureAcks.map(_.withTraceContext { implicit traceContext => signedAck =>
        val ack = signedAck.content
        val member = ack.member
        val timestamp = ack.timestamp
        val error =
          SequencerError.InvalidAcknowledgementTimestamp.Error(member, timestamp, state.lastBlockTs)
        (member, timestamp, error)
      })
      sigChecks <- FutureUnlessShutdown.outcomeF(Future.sequence(goodTsAcks.map(_.withTraceContext {
        implicit traceContext => signedAck =>
          val ack = signedAck.content
          signedAck
            .verifySignature(
              snapshot,
              ack.member,
              HashPurpose.AcknowledgementSignature,
            )
            .leftMap(error =>
              (
                ack.member,
                ack.timestamp,
                SequencerError.InvalidAcknowledgementSignature
                  .Error(signedAck, state.lastBlockTs, error): BaseAlarm,
              )
            )
            .map(_ => (ack.member, ack.timestamp))
      }.value)))
      (invalidSigAcks, validSigAcks) = sigChecks.separate
      acksByMember = validSigAcks
        // Look for the highest acked timestamp by each member
        .groupBy { case (member, _) => member }
        .fmap(NonEmptyUtil.fromUnsafe(_).maxBy1(_._2)._2)
    } yield (acksByMember, invalidTsAcks ++ invalidSigAcks)

  private def recordSubmissionMetrics(
      value: Seq[Traced[LedgerBlockEvent]]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      value.foreach(_.withTraceContext { implicit traceContext =>
        {
          case LedgerBlockEvent.Send(_, signedSubmissionRequest, payloadSize) =>
            val mc = SequencerMetrics.submissionTypeMetricsContext(
              signedSubmissionRequest.submissionRequest.batch.allRecipients,
              signedSubmissionRequest.submissionRequest.sender,
              logger,
            )
            metrics.block.blockEvents.mark()(mc)
            metrics.block.blockEventBytes.mark(payloadSize.longValue)(mc)

          case LedgerBlockEvent.Acknowledgment(request) =>
            // record the event
            metrics.block.blockEvents
              .mark()(
                MetricsContext(
                  "member" -> request.content.member.toString,
                  "type" -> "ack",
                )
              )
            // record the timestamp of the acknowledgment
            metrics.block
              .updateAcknowledgementGauge(
                request.content.member.toString,
                request.content.timestamp.underlying.micros,
              )
        }
      })
    }
}
