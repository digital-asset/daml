// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.implicits.catsStdInstancesForFuture
import cats.syntax.alternative.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.LedgerBlockEvent
import com.digitalasset.canton.domain.block.LedgerBlockEvent.{Acknowledgment, Send}
import com.digitalasset.canton.domain.block.update.BlockUpdateGeneratorImpl.{
  RecipientStats,
  SequencedSubmission,
  State,
}
import com.digitalasset.canton.domain.block.update.SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import com.digitalasset.canton.domain.metrics.BlockMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

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
    metrics: BlockMetrics,
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
    )

  def processChunk(
      state: BlockUpdateGeneratorImpl.State,
      height: Long,
      index: Int,
      chunkEvents: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(BlockUpdateGeneratorImpl.State, ChunkUpdate[UnsignedChunkEvents])] = {
    val (lastTsBeforeValidation, fixedTsChanges) = fixTimestamps(height, index, state, chunkEvents)

    // TODO(i18438): verify the signature of the sequencer on the SendEvent
    val submissionRequests =
      fixedTsChanges.collect { case (ts, ev @ Traced(sendEvent: Send)) =>
        // Discard the timestamp of the `Send` event as this one is obsolete
        (ts, ev.map(_ => sendEvent.signedSubmissionRequest))
      }

    FutureUtil.doNotAwait(
      recordSubmissionMetrics(fixedTsChanges.map(_._2)),
      "submission metric updating failed",
    )

    for {
      sequencedSubmissionsWithSnapshots <-
        addSnapshots(
          state.latestSequencerEventTimestamp,
          state.ephemeral.headCounter(sequencerId),
          submissionRequests,
        )
      newMembers <- detectMembersWithoutSequencerCounters(state, sequencedSubmissionsWithSnapshots)
      _ = if (newMembers.nonEmpty) {
        logger.info(s"Detected new members without sequencer counter: $newMembers")
      }

      acksValidationResult <- processAcknowledgements(state, fixedTsChanges)
      (acksByMember, invalidAcks) = acksValidationResult

      // Warn if we use an approximate snapshot but only after we've read at least one
      warnIfApproximate = state.ephemeral.headCounterAboveGenesis(sequencerId)

      newMembersTraffic <-
        computeNewMembersTraffic(
          state,
          lastTsBeforeValidation,
          newMembers,
          warnIfApproximate,
        )
      stateWithNewMembers = addNewMembers(
        state,
        height,
        index,
        newMembers,
        acksByMember,
        newMembersTraffic,
      )

      validationResult <-
        sequencedSubmissionsValidator.validateSequencedSubmissions(
          stateWithNewMembers,
          height,
          sequencedSubmissionsWithSnapshots,
        )
      SequencedSubmissionsValidationResult(
        finalEphemeralState,
        reversedSignedEvents,
        inFlightAggregationUpdates,
        lastSequencerEventTimestamp,
        reversedOutcomes,
      ) = validationResult

      finalEphemeralStateWithAggregationExpiry =
        finalEphemeralState.evictExpiredInFlightAggregations(lastTsBeforeValidation)
      chunkUpdate =
        ChunkUpdate(
          newMembers,
          acksByMember,
          invalidAcks,
          reversedSignedEvents.reverse,
          inFlightAggregationUpdates,
          lastSequencerEventTimestamp,
          finalEphemeralStateWithAggregationExpiry,
          reversedOutcomes.reverse,
        )

      // We don't want to take into consideration events that have possibly been discarded, otherwise we could
      // assign a last ts value to the block based on an event that wasn't included in the block which would cause
      // validations to fail down the line. That's why we need to compute it using only validated events, instead of
      // using the lastTs computed initially pre-validation.
      lastChunkTsOfSuccessfulEvents =
        reversedSignedEvents
          .map(_.sequencingTimestamp)
          .maxOption
          .orElse(newMembers.values.maxOption)
          .getOrElse(state.lastChunkTs)

      newState =
        BlockUpdateGeneratorImpl.State(
          state.lastBlockTs,
          lastChunkTsOfSuccessfulEvents,
          lastSequencerEventTimestamp.orElse(state.latestSequencerEventTimestamp),
          finalEphemeralStateWithAggregationExpiry,
        )
    } yield (newState, chunkUpdate)
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
      submissionRequests: Seq[(CantonTimestamp, Traced[SignedContent[SubmissionRequest]])],
  )(implicit executionContext: ExecutionContext): FutureUnlessShutdown[Seq[SequencedSubmission]] =
    submissionRequests.parTraverse { case (sequencingTimestamp, tracedSubmissionRequest) =>
      tracedSubmissionRequest.withTraceContext { implicit traceContext => submissionRequest =>
        // Warn if we use an approximate snapshot but only after we've read at least one
        val warnIfApproximate = sequencersSequencerCounter.exists(_ > SequencerCounter.Genesis)
        for {
          sequencingSnapshot <- SyncCryptoClient.getSnapshotForTimestampUS(
            domainSyncCryptoApi,
            sequencingTimestamp,
            latestSequencerEventTimestamp,
            protocolVersion,
            warnIfApproximate,
          )
          topologySnapshotO <- submissionRequest.content.topologyTimestamp match {
            case None => FutureUnlessShutdown.pure(None)
            case Some(topologyTimestamp) if topologyTimestamp <= sequencingTimestamp =>
              SyncCryptoClient
                .getSnapshotForTimestampUS(
                  domainSyncCryptoApi,
                  topologyTimestamp,
                  latestSequencerEventTimestamp,
                  protocolVersion,
                  warnIfApproximate,
                )
                .map(Some(_))
            case _ => FutureUnlessShutdown.pure(None)
          }
        } yield SequencedSubmission(
          sequencingTimestamp,
          submissionRequest,
          sequencingSnapshot,
          topologySnapshotO,
        )(traceContext)
      }
    }

  private def computeNewMembersTraffic(
      state: State,
      lastTsBeforeValidation: CantonTimestamp,
      newMembers: Map[Member, CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[_ <: Member, TrafficState]] =
    if (newMembers.nonEmpty) {
      // We are using the snapshot at lastTs for all new members in this chunk rather than their registration times.
      // In theory, a parameter change could have become effective in between, but we deliberately ignore this for now.
      // Moreover, a member is effectively registered when it appears in the topology state with the relevant certificate,
      // but the traffic state here is created only when the member sends or receives the first message.
      for {
        snapshot <- SyncCryptoClient
          .getSnapshotForTimestampUS(
            client = domainSyncCryptoApi,
            desiredTimestamp = lastTsBeforeValidation,
            previousTimestampO = state.latestSequencerEventTimestamp,
            protocolVersion = protocolVersion,
            warnIfApproximate = warnIfApproximate,
          )
        parameters <- snapshot.ipsSnapshot.trafficControlParameters(protocolVersion)
        updatedStates <- parameters match {
          case Some(params) =>
            newMembers.toList
              .parTraverse { case (member, timestamp) =>
                rateLimitManager
                  .createNewTrafficStateAt(
                    member,
                    timestamp.immediatePredecessor,
                    params,
                  )
                  .map(member -> _)
              }
              .map(_.toMap)
          case _ =>
            FutureUnlessShutdown.pure(
              newMembers.view.mapValues { timestamp =>
                TrafficState.empty(timestamp)
              }.toMap
            )
        }
      } yield updatedStates
    } else FutureUnlessShutdown.pure(Map.empty)

  private def addNewMembers(
      state: State,
      height: Long,
      index: Int,
      newMembers: Map[Member, CantonTimestamp],
      acksByMember: Map[Member, CantonTimestamp],
      newMembersTraffic: Map[_ <: Member, TrafficState],
  )(implicit traceContext: TraceContext): State = {
    val newMemberStatus = newMembers.map { case (member, ts) =>
      member -> InternalSequencerMemberStatus(ts, None)
    }

    val newMembersWithAcknowledgements =
      acksByMember.foldLeft(state.ephemeral.membersMap ++ newMemberStatus) {
        case (membersMap, (member, timestamp)) =>
          membersMap
            .get(member)
            .fold {
              logger.debug(
                s"Ack at $timestamp for $member (block $height, chunk $index) being ignored because the member has not yet been registered."
              )
              membersMap
            } { memberStatus =>
              membersMap.updated(member, memberStatus.copy(lastAcknowledged = Some(timestamp)))
            }
      }

    state
      .focus(_.ephemeral.membersMap)
      .replace(newMembersWithAcknowledgements)
      .focus(_.ephemeral.trafficState)
      .modify(_ ++ newMembersTraffic)
  }

  private def detectMembersWithoutSequencerCounters(
      state: BlockUpdateGeneratorImpl.State,
      sequencedSubmissions: Seq[SequencedSubmission],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[Member, CantonTimestamp]] =
    sequencedSubmissions
      .parFoldMapA { sequencedSubmission =>
        val SequencedSubmission(sequencingTimestamp, event, sequencingSnapshot, topologySnapshotO) =
          sequencedSubmission

        def recipientIsKnown(member: Member): Future[Option[Member]] = {
          sequencingSnapshot.ipsSnapshot
            .isMemberKnown(member)
            .map(Option.when(_)(member))
        }

        val topologySnapshot = topologySnapshotO.getOrElse(sequencingSnapshot).ipsSnapshot
        import event.content.sender
        for {
          groupToMembers <- FutureUnlessShutdown.outcomeF(
            GroupAddressResolver.resolveGroupsToMembers(
              event.content.batch.allRecipients.collect { case groupRecipient: GroupRecipient =>
                groupRecipient
              },
              topologySnapshot,
            )
          )
          memberRecipients = event.content.batch.allRecipients.collect {
            case MemberRecipient(member) => member
          }
          eligibleSenders = event.content.aggregationRule.fold(Seq.empty[Member])(
            _.eligibleSenders
          )
          knownMemberRecipientsOrSender <- FutureUnlessShutdown.outcomeF(
            (eligibleSenders ++ memberRecipients.toSeq :+ sender)
              .parTraverseFilter(recipientIsKnown)
          )
        } yield {
          val knownGroupMembers = groupToMembers.values.flatten

          val allMembersInSubmission =
            Set.empty ++ knownGroupMembers ++ knownMemberRecipientsOrSender
          (allMembersInSubmission -- state.ephemeral.registeredMembers)
            .map(_ -> sequencingTimestamp)
            .toSeq
        }
      }
      .map(
        _.groupBy { case (member, _) => member }
          .mapFilter { tssForMember => tssForMember.map { case (_, ts) => ts }.minOption }
      )

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
            signedSubmissionRequest.content.content.batch.allRecipients
              .foldLeft(RecipientStats()) {
                case (acc, MemberRecipient(ParticipantId(_)) | ParticipantsOfParty(_)) =>
                  acc.copy(participants = true)
                case (acc, MemberRecipient(MediatorId(_)) | MediatorGroupRecipient(_)) =>
                  acc.copy(mediators = true)
                case (acc, MemberRecipient(SequencerId(_)) | SequencersOfDomain) =>
                  acc.copy(sequencers = true)
                case (acc, AllMembersOfDomain) => acc.copy(broadcast = true)
              }
              .updateMetric(
                signedSubmissionRequest.content.content.sender,
                payloadSize,
                logger,
                metrics,
              )
          case LedgerBlockEvent.Acknowledgment(request) =>
            // record the event
            metrics.blockEvents
              .mark()(
                MetricsContext(
                  "sender" -> request.content.member.toString,
                  "type" -> "ack",
                )
              )
            // record the timestamp of the acknowledgment
            metrics
              .updateAcknowledgementGauge(
                request.content.member.toString,
                request.content.timestamp.underlying.micros,
              )
        }
      })
    }
}
