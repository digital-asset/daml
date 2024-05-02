// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.data.{Chain, EitherT, OptionT}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.crypto.{
  DomainSyncCryptoClient,
  HashPurpose,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.LedgerBlockEvent.*
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  BlockUpdateEphemeralState,
}
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SignedOrderingRequestOps
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.InvalidLedgerEvent
import com.digitalasset.canton.domain.sequencing.sequencer.store.CounterCheckpoint
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.client.SequencedEventValidator.TopologyTimestampVerificationError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DomainId, Member, PartyId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, IterableUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/** Exposes functions that take the deserialized contents of a block from a blockchain integration
  * and computes the new [[com.digitalasset.canton.domain.block.BlockUpdate]]s.
  *
  * In particular, these functions are responsible for the final timestamp assignment of a given submission request.
  * The timestamp assignment works as follows:
  * 1. an initial timestamp is assigned to the submission request by the sequencer that writes it to the ledger
  * 2. each sequencer that reads the block potentially adapts the previously assigned timestamp
  * deterministically via `ensureStrictlyIncreasingTimestamp`
  * 3. this timestamp is used to compute the [[com.digitalasset.canton.domain.block.BlockUpdate]]s
  *
  * Reasoning:
  * Step 1 is done so that every sequencer sees the same timestamp for a given event.
  * Step 2 is needed because different sequencers may assign the same timestamps to different events or may not assign
  * strictly increasing timestamps due to clock skews.
  *
  * Invariants:
  * For step 2, we assume that every sequencer observes the same stream of events from the underlying ledger
  * (and especially that events are always read in the same order).
  */
trait BlockUpdateGenerator {
  import BlockUpdateGenerator.*

  type InternalState

  def internalStateFor(state: BlockEphemeralState): InternalState

  def extractBlockEvents(block: RawLedgerBlock): BlockEvents

  def chunkBlock(block: BlockEvents)(implicit
      traceContext: TraceContext
  ): immutable.Iterable[BlockChunk]

  def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate[UnsignedChunkEvents])]

  def signChunkEvents(events: UnsignedChunkEvents)(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[SignedChunkEvents]
}

object BlockUpdateGenerator {

  type EventsForSubmissionRequest = Map[Member, SequencedEvent[ClosedEnvelope]]

  type SignedEvents = Map[Member, OrdinarySerializedEvent]

  sealed trait BlockChunk extends Product with Serializable
  final case class NextChunk(
      blockHeight: Long,
      chunkIndex: Int,
      events: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  ) extends BlockChunk
  final case class EndOfBlock(blockHeight: Long) extends BlockChunk
}

class BlockUpdateGeneratorImpl(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    maybeLowerTopologyTimestampBound: Option[CantonTimestamp],
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    protected val loggerFactory: NamedLoggerFactory,
    unifiedSequencer: Boolean,
)(implicit val closeContext: CloseContext)
    extends BlockUpdateGenerator
    with NamedLogging {

  import com.digitalasset.canton.domain.block.BlockUpdateGenerator.*
  import com.digitalasset.canton.domain.block.BlockUpdateGeneratorImpl.*

  override def extractBlockEvents(block: RawLedgerBlock): BlockEvents = {
    val ledgerBlockEvents = block.events.mapFilter { tracedEvent =>
      implicit val traceContext: TraceContext = tracedEvent.traceContext
      LedgerBlockEvent.fromRawBlockEvent(protocolVersion)(tracedEvent.value) match {
        case Left(error) =>
          InvalidLedgerEvent.Error(block.blockHeight, error).discard
          None
        case Right(value) =>
          Some(Traced(value))
      }
    }
    BlockEvents(block.blockHeight, ledgerBlockEvents)
  }

  override type InternalState = State
  private[block] case class State(
      lastBlockTs: CantonTimestamp,
      lastChunkTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      ephemeral: BlockUpdateEphemeralState,
  )

  override def internalStateFor(state: BlockEphemeralState): InternalState = State(
    lastBlockTs = state.latestBlock.lastTs,
    lastChunkTs = state.latestBlock.lastTs,
    latestSequencerEventTimestamp = state.latestBlock.latestSequencerEventTimestamp,
    ephemeral = state.state.toBlockUpdateEphemeralState,
  )

  override def chunkBlock(
      block: BlockEvents
  )(implicit traceContext: TraceContext): immutable.Iterable[BlockChunk] = {
    // We must start a new chunk whenever the chunk processing advances lastSequencerEventTimestamp
    // Otherwise the logic for retrieving a topology snapshot or traffic state could deadlock
    def possibleEventToThisSequencer(event: LedgerBlockEvent): Boolean = event match {
      case Send(_, signedOrderingRequest) =>
        val allRecipients =
          signedOrderingRequest.signedSubmissionRequest.content.batch.allRecipients
        allRecipients.contains(AllMembersOfDomain) ||
        allRecipients.contains(SequencersOfDomain)
      case _ => false
    }

    val blockHeight = block.height

    IterableUtil
      .splitAfter(block.events)(event => possibleEventToThisSequencer(event.value))
      .zipWithIndex
      .map { case (events, index) =>
        NextChunk(blockHeight, index, events)
      } ++ Seq(EndOfBlock(blockHeight))
  }

  override final def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate[UnsignedChunkEvents])] = {
    chunk match {
      case EndOfBlock(height) =>
        val newState = state.copy(lastBlockTs = state.lastChunkTs)
        val update = CompleteBlockUpdate(
          BlockInfo(height, state.lastChunkTs, state.latestSequencerEventTimestamp)
        )
        FutureUnlessShutdown.pure(newState -> update)
      case NextChunk(height, index, events) =>
        processChunk(height, index, state, events)
    }
  }

  private def processChunk(
      height: Long,
      index: Int,
      state: State,
      chunk: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(State, ChunkUpdate[UnsignedChunkEvents])] = {
    val (lastTsBeforeValidation, revFixedTsChanges) =
      // With this logic, we assign to the initial non-Send events the same timestamp as for the last
      // block. This means that we will include these events in the ephemeral state of the previous block
      // when we re-read it from the database. But this doesn't matter given that all those events are idempotent.
      chunk.forgetNE.foldLeft[
        (CantonTimestamp, Seq[(CantonTimestamp, Traced[LedgerBlockEvent])]),
      ]((state.lastChunkTs, Seq.empty)) { case ((lastTs, events), event) =>
        event.value match {
          case send: LedgerBlockEvent.Send =>
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

    // TODO(i18438): verify the signature of the sequencer on the SendEvent
    val submissionRequests = fixedTsChanges.collect { case (ts, ev @ Traced(sendEvent: Send)) =>
      // Discard the timestamp of the `Send` event as this one is obsolete
      (ts, ev.map(_ => sendEvent.signedSubmissionRequest))
    }

    for {
      submissionRequestsWithSnapshots <- addSnapshots(
        state.latestSequencerEventTimestamp,
        state.ephemeral.headCounter(sequencerId),
        submissionRequests,
      )
      newMembers <- detectMembersWithoutSequencerCounters(submissionRequestsWithSnapshots, state)
      _ = if (newMembers.nonEmpty) {
        logger.info(s"Detected new members without sequencer counter: $newMembers")
      }
      validatedAcks <- processAcknowledgements(state, fixedTsChanges)
      (acksByMember, invalidAcks) = validatedAcks
      // Warn if we use an approximate snapshot but only after we've read at least one
      warnIfApproximate = state.ephemeral.headCounterAboveGenesis(sequencerId)
      newMembersTraffic <-
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
      stateWithNewMembers = {
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
      result <- MonadUtil.foldLeftM(
        (
          Seq.empty[UnsignedChunkEvents],
          Map.empty[AggregationId, InFlightAggregationUpdate],
          stateWithNewMembers.ephemeral,
          Option.empty[CantonTimestamp],
          Seq.empty[SubmissionRequestOutcome],
        ),
        submissionRequestsWithSnapshots,
      )(validateSubmissionRequestAndAddEvents(height, state.latestSequencerEventTimestamp))
    } yield {
      val (
        reversedSignedEvents,
        inFlightAggregationUpdates,
        finalEphemeralState,
        lastSequencerEventTimestamp,
        reversedOutcomes,
      ) = result
      val finalEphemeralStateWithAggregationExpiry =
        finalEphemeralState.evictExpiredInFlightAggregations(lastTsBeforeValidation)
      val chunkUpdate = ChunkUpdate(
        newMembers,
        acksByMember,
        invalidAcks,
        reversedSignedEvents.reverse,
        inFlightAggregationUpdates,
        lastSequencerEventTimestamp,
        finalEphemeralStateWithAggregationExpiry,
        reversedOutcomes.reverse,
      )

      // we don't want to take into consideration events that have possibly been discarded, otherwise we could
      // assign a last ts value to the block based on an event that wasn't included in the block which would cause
      // validations to fail down the line. That's why we need to compute it using only validated events, instead of
      // using the lastTs computed initially pre-validation.
      val lastChunkTsOfSuccessfulEvents = reversedSignedEvents
        .map(_.sequencingTimestamp)
        .maxOption
        .orElse(newMembers.values.maxOption)
        .getOrElse(state.lastChunkTs)

      val newState = State(
        state.lastBlockTs,
        lastChunkTsOfSuccessfulEvents,
        lastSequencerEventTimestamp.orElse(state.latestSequencerEventTimestamp),
        finalEphemeralStateWithAggregationExpiry,
      )
      (newState, chunkUpdate)
    }
  }

  private def addSnapshots(
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      sequencersSequencerCounter: Option[SequencerCounter],
      submissionRequests: Seq[(CantonTimestamp, Traced[SignedContent[SubmissionRequest]])],
  )(implicit executionContext: ExecutionContext): FutureUnlessShutdown[Seq[SequencedSubmission]] = {
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
  }

  private def detectMembersWithoutSequencerCounters(
      sequencedSubmissions: Seq[SequencedSubmission],
      state: State,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[Member, CantonTimestamp]] = {
    sequencedSubmissions
      .parFoldMapA { sequencedSubmission =>
        val SequencedSubmission(sequencingTimestamp, event, sequencingSnapshot, topologySnapshotO) =
          sequencedSubmission

        def recipientIsKnown(member: Member): Future[Option[Member]] = {
          if (!member.isAuthenticated) Future.successful(None)
          else
            sequencingSnapshot.ipsSnapshot
              .isMemberKnown(member)
              .map(Option.when(_)(member))
        }

        val topologySnapshot = topologySnapshotO.getOrElse(sequencingSnapshot).ipsSnapshot
        import event.content.sender
        for {
          groupToMembers <- FutureUnlessShutdown.outcomeF(
            groupAddressResolver.resolveGroupsToMembers(
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
          val allowUnauthenticatedSender = Option.when(!sender.isAuthenticated)(sender).toList

          val allMembersInSubmission =
            Set.empty ++ knownGroupMembers ++ knownMemberRecipientsOrSender ++ allowUnauthenticatedSender
          (allMembersInSubmission -- state.ephemeral.registeredMembers)
            .map(_ -> sequencingTimestamp)
            .toSeq
        }
      }
      .map(
        _.groupBy { case (member, _) => member }
          .mapFilter { tssForMember => tssForMember.map { case (_, ts) => ts }.minOption }
      )
  }

  private def processAcknowledgements(
      state: State,
      fixedTsChanges: Seq[(CantonTimestamp, Traced[LedgerBlockEvent])],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    (Map[Member, CantonTimestamp], Seq[(Member, CantonTimestamp, BaseAlarm)])
  ] = {
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
  }

  /** @param latestSequencerEventTimestamp
    * Since each chunk contains at most one event addressed to the sequencer,
    * (and if so it's the last event), we can treat this timestamp static for the whole chunk and
    * need not update it in the accumulator.
    */
  private def validateSubmissionRequestAndAddEvents(
      height: Long,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(
      acc: (
          Seq[UnsignedChunkEvents],
          InFlightAggregationUpdates,
          BlockUpdateEphemeralState,
          Option[CantonTimestamp],
          Seq[SubmissionRequestOutcome],
      ),
      sequencedSubmissionRequest: SequencedSubmission,
  )(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[
    (
        Seq[UnsignedChunkEvents],
        InFlightAggregationUpdates,
        BlockUpdateEphemeralState,
        Option[CantonTimestamp],
        Seq[SubmissionRequestOutcome],
    )
  ] = {
    val (
      reversedEvents,
      inFlightAggregationUpdates,
      stFromAcc,
      sequencerEventTimestampSoFarO,
      revOutcomes,
    ) = acc
    val SequencedSubmission(
      sequencingTimestamp,
      signedSubmissionRequest,
      sequencingSnapshot,
      signingSnapshotO,
    ) = sequencedSubmissionRequest
    implicit val traceContext: TraceContext = sequencedSubmissionRequest.traceContext

    ErrorUtil.requireState(
      sequencerEventTimestampSoFarO.isEmpty,
      "Only the last event in a chunk could be addressed to the sequencer",
    )

    def processSubmissionOutcome(
        st: BlockUpdateEphemeralState,
        outcome: SubmissionRequestOutcome,
        sequencerEventTimestampO: Option[CantonTimestamp],
    ): FutureUnlessShutdown[
      (
          Seq[UnsignedChunkEvents],
          InFlightAggregationUpdates,
          BlockUpdateEphemeralState,
          Option[CantonTimestamp],
          Seq[SubmissionRequestOutcome],
      )
    ] = outcome match {
      case SubmissionRequestOutcome(
            deliverEvents,
            newAggregationO,
            signingSnapshotO,
            _,
          ) =>
        NonEmpty.from(deliverEvents) match {
          case None => // No state update if there is nothing to deliver
            FutureUnlessShutdown.pure(acc)
          case Some(deliverEventsNE) =>
            val newCheckpoints = st.checkpoints ++ deliverEvents.fmap(d =>
              CounterCheckpoint(d.counter, d.timestamp, None)
            ) // ordering of the two operands matters
            val (newInFlightAggregations, newInFlightAggregationUpdates) =
              newAggregationO.fold(st.inFlightAggregations -> inFlightAggregationUpdates) {
                case (aggregationId, inFlightAggregationUpdate) =>
                  InFlightAggregations.tryApplyUpdates(
                    st.inFlightAggregations,
                    Map(aggregationId -> inFlightAggregationUpdate),
                    ignoreInFlightAggregationErrors = false,
                  ) ->
                    MapsUtil.extendedMapWith(
                      inFlightAggregationUpdates,
                      Iterable(aggregationId -> inFlightAggregationUpdate),
                    )(_ tryMerge _)
              }
            val newState =
              st.copy(inFlightAggregations = newInFlightAggregations, checkpoints = newCheckpoints)

            // If we haven't yet computed a snapshot for signing,
            // we now get one for the sequencing timestamp
            val signingSnapshot = signingSnapshotO.getOrElse(sequencingSnapshot)
            for {
              // Update the traffic status of the recipients before generating the events below.
              // Typically traffic state might change even for recipients if a top up becomes effective at that timestamp
              // Doing this here ensures that the traffic state persisted for the event is correct
              // It's also important to do this here after group -> Set[member] resolution has been performed so we get
              // the actual member recipients
              trafficUpdatedState <- updateTrafficStates(
                newState,
                deliverEventsNE.keySet,
                sequencingTimestamp,
                sequencingSnapshot,
                latestSequencerEventTimestamp,
              )
            } yield {
              val unsignedEvents = UnsignedChunkEvents(
                signedSubmissionRequest.content.sender,
                deliverEventsNE,
                signingSnapshot,
                sequencingTimestamp,
                sequencingSnapshot,
                trafficUpdatedState.trafficState.view.mapValues(_.toSequencedEventTrafficState),
                traceContext,
              )
              (
                unsignedEvents +: reversedEvents,
                newInFlightAggregationUpdates,
                trafficUpdatedState,
                sequencerEventTimestampO,
                outcome +: revOutcomes,
              )
            }
        }
    }

    for {
      newStateAndOutcome <- validateAndGenerateSequencedEvents(
        sequencingTimestamp,
        signedSubmissionRequest,
        stFromAcc,
        sequencingSnapshot,
        signingSnapshotO,
        latestSequencerEventTimestamp,
      )
      (newState, outcome, sequencerEventTimestampO) = newStateAndOutcome
      result <- processSubmissionOutcome(newState, outcome, sequencerEventTimestampO)
    } yield {
      logger.debug(
        s"At block $height, the submission request ${signedSubmissionRequest.content.messageId} at $sequencingTimestamp created the following counters: \n" ++ outcome.eventsByMember
          .map { case (member, sequencedEvent) =>
            s"\t counter ${sequencedEvent.counter} for $member"
          }
          .mkString("\n")
      )
      result
    }
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

  private def validateMaxSequencingTimeForAggregationRule(
      submissionRequest: SubmissionRequest,
      topologySnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
      st: BlockUpdateEphemeralState,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] = {
    submissionRequest.aggregationRule.traverse_ { _aggregationRule =>
      for {
        domainParameters <- EitherT(topologySnapshot.ipsSnapshot.findDynamicDomainParameters())
          .leftMap(error =>
            invalidSubmissionRequest(
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"Could not fetch dynamic domain parameters: $error"
              ),
              st.tryNextCounter(submissionRequest.sender),
            )
          )
        maxSequencingTimeUpperBound = sequencingTimestamp.toInstant.plus(
          domainParameters.parameters.sequencerAggregateSubmissionTimeout.duration
        )
        _ <- EitherTUtil.condUnitET[Future](
          submissionRequest.maxSequencingTime.toInstant.isBefore(maxSequencingTimeUpperBound),
          invalidSubmissionRequest(
            submissionRequest,
            sequencingTimestamp,
            SequencerErrors.MaxSequencingTimeTooFar(
              submissionRequest.messageId,
              submissionRequest.maxSequencingTime,
              maxSequencingTimeUpperBound,
            ),
            st.tryNextCounter(submissionRequest.sender),
          ),
        )
      } yield ()
    }
  }

  /** Returns the snapshot for signing the events (if the submission request specifies a signing timestamp)
    * and the sequenced events by member.
    *
    * Drops the submission request if the sender is not registered or
    * the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.maxSequencingTime]]
    * is before the `sequencingTimestamp`.
    *
    * Produces a [[com.digitalasset.canton.sequencing.protocol.DeliverError]]
    * if some recipients are unknown or the requested
    * [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.topologyTimestamp]]
    * is too old or after the `sequencingTime`.
    */
  private def validateAndGenerateSequencedEvents(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      st: BlockUpdateEphemeralState,
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshotO: Option[SyncCryptoApi],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[
    (BlockUpdateEphemeralState, SubmissionRequestOutcome, Option[CantonTimestamp])
  ] = {
    val submissionRequest = signedSubmissionRequest.content

    // Below are a 3 functions, each a for-comprehension of EitherT.
    // In each Lefts are used to stop processing the submission request and immediately produce the sequenced events
    // They are split into 3 functions to make it possible to re-use intermediate results (specifically BlockUpdateEphemeralState
    // containing updated traffic states), even if further processing fails.

    // This first function performs initial validations and resolves groups to members
    def performInitialValidations = for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        st.registeredMembers.contains(submissionRequest.sender),
        (),
        // we expect callers to validate the sender exists before queuing requests on their behalf
        // if we hit this case here it likely means the caller didn't check, or the member has subsequently
        // been deleted.
        {
          logger.warn(
            s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] is not registered so cannot send or receive events. Dropping send request."
          )
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        sequencingTimestamp <= submissionRequest.maxSequencingTime,
        (),
        // The sequencer is beyond the timestamp allowed for sequencing this request so it is silently dropped.
        // A correct sender should be monitoring their sequenced events and notice that the max-sequencing-time has been
        // exceeded and trigger a timeout.
        // We don't log this as a warning as it is expected behaviour. Within a distributed network, the source of
        // a delay can come from different nodes and we should only log this as a warning in a way where we can
        // attribute the delay to a specific node.
        {
          SequencerError.ExceededMaxSequencingTime
            .Error(
              sequencingTimestamp,
              submissionRequest.maxSequencingTime,
              submissionRequest.messageId.unwrap,
            )
            .discard
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        checkRecipientsAreKnown(
          submissionRequest,
          sequencingTimestamp,
          st.registeredMembers,
          st.tryNextCounter,
        )
      )
      // Warn if we use an approximate snapshot but only after we've read at least one
      _ <- checkSignatureOnSubmissionRequest(
        sequencingTimestamp,
        signedSubmissionRequest,
        sequencingSnapshot,
        latestSequencerEventTimestamp,
      )
      _ <- validateTopologyTimestamp(
        sequencingTimestamp,
        submissionRequest,
        latestSequencerEventTimestamp,
        st.headCounter(sequencerId),
        st.tryNextCounter,
      )
      topologySnapshot = signingSnapshotO.getOrElse(sequencingSnapshot)
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTimeForAggregationRule(
        submissionRequest,
        topologySnapshot,
        sequencingTimestamp,
        st,
      ).mapK(FutureUnlessShutdown.outcomeK)
      _ <- checkClosedEnvelopesSignatures(
        signingSnapshotO,
        submissionRequest,
        sequencingTimestamp,
      ).mapK(FutureUnlessShutdown.outcomeK)
      groupToMembers <- computeGroupAddressesToMembers(
        submissionRequest,
        sequencingTimestamp,
        topologySnapshot,
        st,
      )
    } yield groupToMembers

    // This second function consumes traffic for the sender and update the ephemeral state
    def validateAndUpdateTraffic: EitherT[
      FutureUnlessShutdown,
      SubmissionRequestOutcome,
      (Map[GroupRecipient, Set[Member]], BlockUpdateEphemeralState),
    ] = for {
      groupToMembers <- performInitialValidations
      stateAfterTrafficConsume <- updateRateLimiting(
        submissionRequest,
        sequencingTimestamp,
        sequencingSnapshot,
        groupToMembers,
        st,
        latestSequencerEventTimestamp,
        warnIfApproximate = st.headCounterAboveGenesis(sequencerId),
      )
    } yield (groupToMembers, stateAfterTrafficConsume)

    // This last function performs additional checks and runs the aggregation logic
    // If this succeeds, it will produce a SubmissionRequestOutcome containing DeliverEvents
    def finalizeProcessing(
        groupToMembers: Map[GroupRecipient, Set[Member]],
        stateAfterTrafficConsume: BlockUpdateEphemeralState,
    ): EitherT[
      FutureUnlessShutdown,
      SubmissionRequestOutcome,
      (BlockUpdateEphemeralState, SubmissionRequestOutcome, Option[CantonTimestamp]),
    ] = for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        SequencerValidations.checkToAtMostOneMediator(submissionRequest),
        (), {
          SequencerError.MultipleMediatorRecipients
            .Error(submissionRequest, sequencingTimestamp)
            .report()
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      aggregationIdO = submissionRequest.aggregationId(domainSyncCryptoApi.pureCrypto)
      aggregationOutcome <- EitherT.fromEither[FutureUnlessShutdown](
        aggregationIdO.traverse { aggregationId =>
          val inFlightAggregation = stateAfterTrafficConsume.inFlightAggregations.get(aggregationId)
          validateAggregationRuleAndUpdateInFlightAggregation(
            submissionRequest,
            sequencingTimestamp,
            aggregationId,
            inFlightAggregation,
            stateAfterTrafficConsume.registeredMembers,
            stateAfterTrafficConsume.tryNextCounter,
          ).map(inFlightAggregationUpdate =>
            (aggregationId, inFlightAggregationUpdate, inFlightAggregation)
          )
        }
      )
    } yield {
      val aggregatedBatch = aggregationOutcome.fold(submissionRequest.batch) {
        case (aggregationId, inFlightAggregationUpdate, inFlightAggregation) =>
          val updatedInFlightAggregation = InFlightAggregation.tryApplyUpdate(
            aggregationId,
            inFlightAggregation,
            inFlightAggregationUpdate,
            ignoreInFlightAggregationErrors = false,
          )
          submissionRequest.batch
            .focus(_.envelopes)
            .modify(_.lazyZip(updatedInFlightAggregation.aggregatedSignatures).map {
              (envelope, signatures) => envelope.copy(signatures = signatures)
            })
      }

      val topologyTimestampO = submissionRequest.topologyTimestamp
      val events =
        (groupToMembers.values.flatten.toSet ++ submissionRequest.batch.allMembers + submissionRequest.sender).toSeq.map {
          member =>
            val groups = groupToMembers.collect {
              case (groupAddress, members) if members.contains(member) => groupAddress
            }.toSet
            val deliver = Deliver.create(
              stateAfterTrafficConsume.tryNextCounter(member),
              sequencingTimestamp,
              domainId,
              Option.when(member == submissionRequest.sender)(submissionRequest.messageId),
              Batch.filterClosedEnvelopesFor(aggregatedBatch, member, groups),
              topologyTimestampO,
              protocolVersion,
            )
            member -> deliver
        }.toMap
      val members =
        groupToMembers.values.flatten.toSet ++ submissionRequest.batch.allMembers + submissionRequest.sender
      val aggregationUpdate = aggregationOutcome.map {
        case (aggregationId, inFlightAggregationUpdate, _) =>
          aggregationId -> inFlightAggregationUpdate
      }

      // We need to know whether the group of sequencers was addressed in order to update `latestSequencerEventTimestamp`.
      // Simply checking whether this sequencer is within the resulting event recipients opens up
      // the door for a malicious participant to target a single sequencer, which would result in the
      // various sequencers reaching a different value.
      //
      // Currently, the only use cases of addressing a sequencer are:
      //   * via AllMembersOfDomain for topology transactions
      //   * via SequencersOfDomain for traffic control top-up messages
      //
      // Therefore, we check whether this sequencer was addressed via a group address to avoid the above
      // case.
      //
      // NOTE: Pruning concerns
      // ----------------------
      // `latestSequencerEventTimestamp` is relevant for pruning.
      // For the traffic top-ups, we can use the block's last timestamp to signal "safe-to-prune", because
      // the logic to compute the balance based on `latestSequencerEventTimestamp` sits inside the manager
      // and we can make it work together with pruning.
      // For topology, pruning is not yet implemented. However, the logic to compute snapshot timestamps
      // sits outside of the topology processor and so from the topology processor's point of view,
      // `latestSequencerEventTimestamp` should be part of a "safe-to-prune" timestamp calculation.
      //
      // See https://github.com/DACH-NY/canton/pull/17676#discussion_r1515926774
      val sequencerIsAddressed =
        groupToMembers.contains(AllMembersOfDomain) || groupToMembers.contains(SequencersOfDomain)
      val sequencerEventTimestampO = Option.when(sequencerIsAddressed)(sequencingTimestamp)

      (
        stateAfterTrafficConsume,
        SubmissionRequestOutcome(
          events,
          aggregationUpdate,
          signingSnapshotO,
          outcome = SubmissionOutcome.Deliver(
            submissionRequest,
            sequencingTimestamp,
            members,
            aggregatedBatch,
          ),
        ),
        sequencerEventTimestampO,
      )
    }

    // A bit more convoluted than we'd like here, but the goal is to be able to use the traffic updated state in the result,
    // even if the aggregation logic performed in 'finalizeProcessing' short-circuits (for instance because we've already reached the aggregation threshold)
    validateAndUpdateTraffic
      .flatMap { case (groupToMembers, stateAfterTrafficConsume) =>
        finalizeProcessing(groupToMembers, stateAfterTrafficConsume)
          // Use the traffic updated ephemeral state in the response even if the rest of the processing stopped
          .recover { errorSubmissionOutcome =>
            (stateAfterTrafficConsume, errorSubmissionOutcome, None)
          }
      }
      .leftMap { errorSubmissionOutcome =>
        (st, errorSubmissionOutcome, None)
      }
      .merge
  }

  private def checkRecipientsAreKnown(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      registeredMembers: Set[Member],
      nextCounter: Member => SequencerCounter,
  )(implicit traceContext: TraceContext): Either[SubmissionRequestOutcome, Unit] = {
    // group addresses checks are covered separately later on
    val unknownRecipients = submissionRequest.batch.allMembers diff registeredMembers
    Either.cond(
      unknownRecipients.isEmpty,
      (),
      invalidSubmissionRequest(
        submissionRequest,
        sequencingTimestamp,
        SequencerErrors.UnknownRecipients(unknownRecipients.toSeq),
        nextCounter(submissionRequest.sender),
      ),
    )
  }

  private def checkClosedEnvelopesSignatures(
      signingSnapshotO: Option[SyncCryptoApi],
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] = {
    signingSnapshotO match {
      case None =>
        EitherT.right[SubmissionRequestOutcome](Future.unit)
      case Some(signingSnapshot) =>
        submissionRequest.batch.envelopes
          .parTraverse_ { closedEnvelope =>
            closedEnvelope.verifySignatures(
              signingSnapshot,
              submissionRequest.sender,
            )
          }
          .leftMap { error =>
            SequencerError.InvalidEnvelopeSignature
              .Error(
                submissionRequest,
                error,
                sequencingTimestamp,
                signingSnapshot.ipsSnapshot.timestamp,
              )
              .report()
            SubmissionRequestOutcome.discardSubmissionRequest
          }
    }
  }

  private def checkSignatureOnSubmissionRequest(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] = {
    val submissionRequest = signedSubmissionRequest.content

    // If we haven't seen any topology transactions yet, then we cannot verify signatures, so we skip it.
    // In practice this should only happen for the first ever transaction, which contains the initial topology data.
    val skipCheck =
      latestSequencerEventTimestamp.isEmpty || !submissionRequest.sender.isAuthenticated
    if (skipCheck) {
      EitherT.pure[FutureUnlessShutdown, SubmissionRequestOutcome](())
    } else {
      val alarmE = for {
        timestampOfSigningKey <- EitherT.fromEither[FutureUnlessShutdown](
          signedSubmissionRequest.timestampOfSigningKey.toRight[BaseAlarm](
            SequencerError.MissingSubmissionRequestSignatureTimestamp.Error(
              signedSubmissionRequest,
              sequencingTimestamp,
            )
          )
        )
        // We are consciously picking the sequencing timestamp for checking the signature instead of the
        // timestamp of the signing key provided by the submitting sequencer.
        // That's in order to avoid a series of issues, one of which is that if the submitting sequencer is
        // malicious, it could pretend to be late and use an old timestamp for a specific client that would
        // allow a signature with a key that was already rolled (and potentially even compromised)
        // to be successfully verified here.
        //
        // The downside is that a client could be rolling a key at the same time that it has ongoing requests,
        // which could end up not being verified here (depending on the order events end up with) even though
        // it would be expected that they would. But this is a very edge-case scenario and it is recommended that
        // clients use their new keys for a while before rolling the old key to avoid any issues.
        _ <- signedSubmissionRequest
          .verifySignature(
            sequencingSnapshot,
            submissionRequest.sender,
            HashPurpose.SubmissionRequestSignature,
          )
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[BaseAlarm](error =>
            SequencerError.InvalidSubmissionRequestSignature.Error(
              signedSubmissionRequest,
              error,
              sequencingTimestamp,
              // timestampOfSigningKey is not being used for sig check,
              // but it is still useful to be made part of the error message
              timestampOfSigningKey,
            )
          )
      } yield ()

      alarmE.leftMap { alarm =>
        alarm.report()
        SubmissionRequestOutcome.discardSubmissionRequest
      }
    }
  }

  private def validateTopologyTimestamp(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      sequencersSequencerCounter: => Option[SequencerCounter],
      nextCounter: Member => SequencerCounter,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] =
    submissionRequest.topologyTimestamp match {
      case None => EitherT.pure(())
      case Some(topologyTimestamp) =>
        def rejectInvalidTopologyTimestamp(
            reason: TopologyTimestampVerificationError
        ): SubmissionRequestOutcome = {
          val rejection = reason match {
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
          invalidSubmissionRequest(
            submissionRequest,
            sequencingTimestamp,
            rejection,
            nextCounter(submissionRequest.sender),
          )
        }

        // Silence the warning if we have not delivered anything to the sequencer's topology client.
        val warnIfApproximate = sequencersSequencerCounter.exists(_ > SequencerCounter.Genesis)
        SequencedEventValidator
          .validateTopologyTimestampUS(
            domainSyncCryptoApi,
            topologyTimestamp,
            sequencingTimestamp,
            latestSequencerEventTimestamp,
            protocolVersion,
            warnIfApproximate,
          )
          .bimap(
            rejectInvalidTopologyTimestamp,
            signingSnapshot => (),
          )
    }

  private def validateAggregationRuleAndUpdateInFlightAggregation(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      aggregationId: AggregationId,
      inFlightAggregationO: Option[InFlightAggregation],
      registeredMembers: Set[Member],
      nextCounter: Member => SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): Either[SubmissionRequestOutcome, InFlightAggregationUpdate] = {
    def refuse(sequencerError: SequencerDeliverError): SubmissionRequestOutcome =
      invalidSubmissionRequest(
        submissionRequest,
        sequencingTimestamp,
        sequencerError,
        nextCounter(submissionRequest.sender),
      )

    def validate(
        rule: AggregationRule
    ): Either[SubmissionRequestOutcome, (InFlightAggregation, InFlightAggregationUpdate)] = {
      for {
        _ <- SequencerValidations
          .wellformedAggregationRule(submissionRequest.sender, rule)
          .leftMap(message => refuse(SequencerErrors.SubmissionRequestMalformed(message)))
        unregisteredEligibleMembers = rule.eligibleSenders.filterNot(registeredMembers.contains)
        _ <- Either.cond(
          unregisteredEligibleMembers.isEmpty,
          (),
          // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
          refuse(
            SequencerErrors.SubmissionRequestRefused(
              s"Aggregation rule contains unregistered eligible members: $unregisteredEligibleMembers"
            )
          ),
        )
      } yield {
        val fresh = FreshInFlightAggregation(submissionRequest.maxSequencingTime, rule)
        InFlightAggregation.initial(fresh) -> InFlightAggregationUpdate(Some(fresh), Chain.empty)
      }
    }

    for {
      inFlightAggregationAndUpdate <- inFlightAggregationO
        .map(_ -> InFlightAggregationUpdate.empty)
        .toRight(())
        .leftFlatMap { _ =>
          val rule = submissionRequest.aggregationRule.getOrElse(
            ErrorUtil.internalError(
              new IllegalStateException(
                "A submission request with an aggregation id must have an aggregation rule"
              )
            )
          )
          validate(rule)
        }
      (inFlightAggregation, inFlightAggregationUpdate) = inFlightAggregationAndUpdate
      aggregatedSender = AggregatedSender(
        submissionRequest.sender,
        AggregationBySender(
          sequencingTimestamp,
          submissionRequest.batch.envelopes.map(_.signatures),
        ),
      )

      newAggregation <- inFlightAggregation
        .tryAggregate(aggregatedSender)
        .leftMap {
          case InFlightAggregation.AlreadyDelivered(deliveredAt) =>
            val message =
              s"The aggregatable request with aggregation ID $aggregationId was previously delivered at $deliveredAt"
            refuse(SequencerErrors.AggregateSubmissionAlreadySent(message))
          case InFlightAggregation.AggregationStuffing(_, at) =>
            val message =
              s"The sender ${submissionRequest.sender} previously contributed to the aggregatable submission with ID $aggregationId at $at"
            refuse(SequencerErrors.AggregateSubmissionStuffing(message))
        }
      fullInFlightAggregationUpdate = inFlightAggregationUpdate.tryMerge(
        InFlightAggregationUpdate(None, Chain.one(aggregatedSender))
      )
      // If we're not delivering the request to all recipients right now, just send a receipt back to the sender
      _ <- Either.cond(
        newAggregation.deliveredAt.nonEmpty,
        logger.debug(
          s"Aggregation ID $aggregationId has reached its threshold ${newAggregation.rule.threshold} and will be delivered at $sequencingTimestamp."
        ), {
          logger.debug(
            s"Aggregation ID $aggregationId has now ${newAggregation.aggregatedSenders.size} senders aggregated. Threshold is ${newAggregation.rule.threshold.value}."
          )
          val deliverReceiptEvent =
            deliverReceipt(submissionRequest, sequencingTimestamp, nextCounter)
          SubmissionRequestOutcome(
            Map(submissionRequest.sender -> deliverReceiptEvent),
            Some(aggregationId -> fullInFlightAggregationUpdate),
            None,
            outcome = SubmissionOutcome.DeliverReceipt(submissionRequest, sequencingTimestamp),
          )
        },
      )
    } yield fullInFlightAggregationUpdate
  }

  private val groupAddressResolver = new GroupAddressResolver(domainSyncCryptoApi)

  // TODO(#18401): This method should be harmonized with the GroupAddressResolver
  private def computeGroupAddressesToMembers(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: SyncCryptoApi,
      st: BlockUpdateEphemeralState,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val groupRecipients = submissionRequest.batch.allRecipients.collect {
      case group: GroupRecipient =>
        group
    }

    def refuse(error: String) = invalidSubmissionRequest(
      submissionRequest,
      sequencingTimestamp,
      SequencerErrors.SubmissionRequestRefused(error),
      st.tryNextCounter(submissionRequest.sender),
    )

    def memberIsRegistered(member: Member): Boolean =
      st.registeredMembers.contains(member)

    if (groupRecipients.isEmpty) EitherT.rightT(Map.empty)
    else
      for {
        participantsOfParty <- {
          val parties = groupRecipients.collect { case ParticipantsOfParty(party) =>
            party.toLf
          }
          if (parties.isEmpty)
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              _ <- topologySnapshot.ipsSnapshot
                .allHaveActiveParticipants(parties)
                .leftMap(parties =>
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(s"The following parties do not have active participants $parties")
                )
              mapping <- EitherT.right[SubmissionRequestOutcome](
                topologySnapshot.ipsSnapshot.activeParticipantsOfParties(parties.toSeq)
              )
              _ <- mapping.toList.parTraverse { case (party, participants) =>
                val nonRegistered = participants.filterNot(memberIsRegistered)
                EitherT.cond[Future](
                  nonRegistered.isEmpty,
                  (),
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(
                    s"The party $party is hosted on non registered participants $nonRegistered"
                  ),
                )
              }
            } yield mapping.map[GroupRecipient, Set[Member]] { case (party, participants) =>
              ParticipantsOfParty(
                PartyId.tryFromLfParty(party)
              ) -> participants.toSet[Member]
            }
        }.mapK(FutureUnlessShutdown.outcomeK)
        mediatorGroupByMember <- {
          val mediatorGroups = groupRecipients.collect { case MediatorGroupRecipient(group) =>
            group
          }.toSeq
          if (mediatorGroups.isEmpty)
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              groups <- topologySnapshot.ipsSnapshot
                .mediatorGroupsOfAll(mediatorGroups)
                .leftMap(nonExistingGroups =>
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(s"The following mediator groups do not exist $nonExistingGroups")
                )
              _ <- groups.parTraverse { group =>
                val nonRegistered = (group.active ++ group.passive).filterNot(memberIsRegistered)
                EitherT.cond[Future](
                  nonRegistered.isEmpty,
                  (),
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(
                    s"The mediator group ${group.index} contains non registered mediators $nonRegistered"
                  ),
                )
              }
            } yield groups
              .map(group =>
                MediatorGroupRecipient(group.index) -> (group.active.forgetNE ++ group.passive)
                  .toSet[Member]
              )
              .toMap[GroupRecipient, Set[Member]]
        }.mapK(FutureUnlessShutdown.outcomeK)
        allRecipients <- {
          if (!groupRecipients.contains(AllMembersOfDomain)) {
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          } else {
            for {
              allMembers <- EitherT.right[SubmissionRequestOutcome](
                topologySnapshot.ipsSnapshot.allMembers()
              )
              _ <- {
                // this can happen when a
                val nonRegistered = allMembers.filterNot(memberIsRegistered)
                EitherT.cond[Future](
                  nonRegistered.isEmpty,
                  (),
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(
                    s"The broadcast group contains non registered members $nonRegistered"
                  ),
                )
              }
            } yield Map((AllMembersOfDomain: GroupRecipient, allMembers))
          }
        }.mapK(FutureUnlessShutdown.outcomeK)

        sequencersOfDomain <- {
          val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
          if (useSequencersOfDomain) {
            for {
              sequencers <- EitherT(
                topologySnapshot.ipsSnapshot
                  .sequencerGroup()
                  .map(
                    _.fold[Either[SubmissionRequestOutcome, Set[Member]]](
                      // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                      Left(refuse("No sequencer group found"))
                    )(group => Right((group.active.forgetNE ++ group.passive).toSet))
                  )
              )
              _ <- {
                val nonRegistered = sequencers.filterNot(memberIsRegistered)
                EitherT.cond[Future](
                  nonRegistered.isEmpty,
                  (),
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(
                    s"The sequencer group contains non registered sequencers $nonRegistered"
                  ),
                )
              }
            } yield Map((SequencersOfDomain: GroupRecipient) -> sequencers)
          } else
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
        }.mapK(FutureUnlessShutdown.outcomeK)
      } yield participantsOfParty ++ mediatorGroupByMember ++ sequencersOfDomain ++ allRecipients
  }

  override def signChunkEvents(unsignedEvents: UnsignedChunkEvents)(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[SignedChunkEvents] = {
    val UnsignedChunkEvents(
      sender,
      events,
      signingSnapshot,
      sequencingTimestamp,
      sequencingSnapshot,
      trafficStates,
      submissionRequestTraceContext,
    ) = unsignedEvents
    implicit val traceContext: TraceContext = submissionRequestTraceContext
    val signingTimestamp = signingSnapshot.ipsSnapshot.timestamp
    val signedEventsF = maybeLowerTopologyTimestampBound match {
      case Some(bound) if bound > signingTimestamp =>
        // As the required topology snapshot timestamp is older than the lower topology timestamp bound, the timestamp
        // of this sequencer's very first topology snapshot, tombstone the events. This enables subscriptions to signal to
        // subscribers that this sequencer is not in a position to serve the events behind these sequencer counters.
        // Comparing against the lower signing timestamp bound prevents tombstones in "steady-state" sequencing beyond
        // "soon" after initial sequencer onboarding. See #13609
        events.toSeq.parTraverse { case (member, event) =>
          logger.info(
            s"Sequencing tombstone for ${member.identifier} at ${event.timestamp} and ${event.counter}. Sequencer signing key at $signingTimestamp not available before the bound $bound."
          )
          // sign tombstones using key valid at sequencing timestamp as event timestamp has no signing key and we
          // are not sequencing the event anyway, but the tombstone
          val err = DeliverError.create(
            event.counter,
            sequencingTimestamp, // use sequencing timestamp for tombstone
            domainId,
            MessageId(String73.tryCreate("tombstone")), // dummy message id
            SequencerErrors.PersistTombstone(event.timestamp, event.counter),
            protocolVersion,
          )
          for {
            signedContent <- FutureUnlessShutdown.outcomeF(
              SignedContent
                .create(
                  sequencingSnapshot.pureCrypto,
                  sequencingSnapshot,
                  err,
                  Some(sequencingSnapshot.ipsSnapshot.timestamp),
                  HashPurpose.SequencedEventSignature,
                  protocolVersion,
                )
                .valueOr(syncCryptoError =>
                  ErrorUtil.internalError(
                    new RuntimeException(
                      s"Error signing tombstone deliver error: $syncCryptoError"
                    )
                  )
                )
            )
          } yield {
            member -> OrdinarySequencedEvent(signedContent, None)(traceContext)
          }
        }
      case _ =>
        FutureUnlessShutdown.outcomeF(
          events.toSeq
            .parTraverse { case (member, event) =>
              SignedContent
                .create(
                  signingSnapshot.pureCrypto,
                  signingSnapshot,
                  event,
                  None,
                  HashPurpose.SequencedEventSignature,
                  protocolVersion,
                )
                .valueOr(syncCryptoError =>
                  ErrorUtil.internalError(
                    new RuntimeException(s"Error signing events: $syncCryptoError")
                  )
                )
                .map { signedContent =>
                  // only include traffic state for the sender
                  val trafficStateO = Option.when(!unifiedSequencer || member == sender) {
                    trafficStates.getOrElse(
                      member,
                      ErrorUtil.invalidState(s"Sender $member unknown by rate limiter."),
                    )
                  }
                  member ->
                    OrdinarySequencedEvent(signedContent, trafficStateO)(traceContext)
                }
            }
        )
    }
    signedEventsF.map(signedEvents => SignedChunkEvents(signedEvents.toMap))
  }

  private def invalidSubmissionRequest(
      request: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencerError: SequencerDeliverError,
      nextCounter: SequencerCounter,
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    val SubmissionRequest(sender, messageId, _, _, _, _, _) = request
    logger.debug(
      show"Rejecting submission request $messageId from $sender with error ${sequencerError.code
          .toMsg(sequencerError.cause, correlationId = None, limit = None)}"
    )
    SubmissionRequestOutcome.reject(
      request,
      sender,
      DeliverError.create(
        nextCounter,
        sequencingTimestamp,
        domainId,
        messageId,
        sequencerError,
        protocolVersion,
      ),
    )
  }

  private def deliverReceipt(
      request: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      nextCounter: Member => SequencerCounter,
  ): SequencedEvent[ClosedEnvelope] = {
    Deliver.create(
      nextCounter(request.sender),
      sequencingTimestamp,
      domainId,
      Some(request.messageId),
      Batch.empty(protocolVersion),
      // Since the receipt does not contain any envelopes and does not authenticate the envelopes
      // in any way, there is no point in including a topology timestamp in the receipt,
      // as it cannot be used to prove anything about the submission anyway.
      None,
      protocolVersion,
    )
  }

  private def updateTrafficStates(
      ephemeralState: BlockUpdateEphemeralState,
      members: Set[Member],
      sequencingTimestamp: CantonTimestamp,
      snapshot: SyncCryptoApi,
      latestTopologyTimestamp: Option[CantonTimestamp],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[BlockUpdateEphemeralState] = {
    snapshot.ipsSnapshot
      .trafficControlParameters(protocolVersion)
      .flatMap {
        case Some(parameters) =>
          val states = members
            .flatMap(member => ephemeralState.trafficState.get(member).map(member -> _))
            .toMap
          rateLimitManager
            .getUpdatedTrafficStatesAtTimestamp(
              states,
              sequencingTimestamp,
              parameters,
              latestTopologyTimestamp,
              warnIfApproximate = ephemeralState.headCounterAboveGenesis(sequencerId),
            )
            .map { trafficStateUpdates =>
              ephemeralState
                .copy(trafficState =
                  ephemeralState.trafficState ++ trafficStateUpdates.view.mapValues(_.state).toMap
                )
            }
        case _ => FutureUnlessShutdown.pure(ephemeralState)
      }
  }

  private def updateRateLimiting(
      request: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencingSnapshot: SyncCryptoApi,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      ephemeralState: BlockUpdateEphemeralState,
      lastSeenTopologyTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, BlockUpdateEphemeralState] = {
    val newStateOF = for {
      _ <- OptionT.when[FutureUnlessShutdown, Unit]({
        request.sender match {
          // Sequencers are not rate limited
          case _: SequencerId => false
          case _ => true
        }
      })(())
      parameters <- OptionT(
        sequencingSnapshot.ipsSnapshot.trafficControlParameters(protocolVersion)
      )
      sender = request.sender
      // Get the traffic from the ephemeral state
      trafficState = ephemeralState.trafficState.getOrElse(
        sender,
        // If there's no trace of this member. But we have ensured that the sender is registered
        // and all registered members should have a traffic state.
        ErrorUtil.invalidState(s"Sender $sender unknown by rate limiter."),
      )
      _ <-
        if (sequencingTimestamp <= trafficState.timestamp) {
          logger.warn(
            s"Trying to consume an event with a sequencing timestamp ($sequencingTimestamp)" +
              s" <= to the current traffic state timestamp ($trafficState)."
          )
          OptionT.none[FutureUnlessShutdown, Unit]
        } else OptionT.some[FutureUnlessShutdown](())
      _ = logger.trace(
        s"Consuming traffic cost for event with messageId ${request.messageId}" +
          s" from sender $sender at $sequencingTimestamp"
      )
      // Consume traffic for the sender
      newSenderTrafficState <- OptionT.liftF(
        rateLimitManager
          .consume(
            request.sender,
            request.batch,
            sequencingTimestamp,
            trafficState,
            parameters,
            groupToMembers,
            lastBalanceUpdateTimestamp = lastSeenTopologyTimestamp,
            warnIfApproximate = warnIfApproximate,
          )
          .map(Right(_))
          .valueOr {
            case error: SequencerRateLimitError.EventOutOfOrder =>
              logger.warn(
                s"Consumed an event out of order for member ${error.member} with traffic state '$trafficState'. Current traffic state timestamp is ${error.currentTimestamp} but event timestamp is ${error.eventTimestamp}. The traffic state will not be updated."
              )
              Right(trafficState)
            case error: SequencerRateLimitError.AboveTrafficLimit
                if parameters.enforceRateLimiting =>
              logger.info(
                s"Submission from member ${error.member} with traffic state '${error.trafficState.toString}' was above traffic limit. Submission cost: ${error.trafficCost.value}. The message will not be delivered."
              )
              Left(
                SubmissionRequestOutcome.reject(
                  request,
                  sender,
                  DeliverError.create(
                    ephemeralState.tryNextCounter(sender),
                    sequencingTimestamp,
                    domainId,
                    request.messageId,
                    SequencerErrors
                      .TrafficCredit(
                        s"Not enough traffic credit for sender $sender to send message with ID ${request.messageId}: $error"
                      ),
                    protocolVersion,
                  ),
                )
              )
            case error: SequencerRateLimitError.AboveTrafficLimit =>
              logger.info(
                s"Submission from member ${error.member} with traffic state '${error.trafficState.toString}' was above traffic limit. Submission cost: ${error.trafficCost.value}. The message will still be delivered."
              )
              Right(error.trafficState)
            case error: SequencerRateLimitError.UnknownBalance =>
              logger.warn(
                s"Could not obtain valid balance at $sequencingTimestamp for member ${error.member} with traffic state '$trafficState'. The message will still be delivered but the traffic state has not been updated."
              )
              Right(trafficState)
          }
      )
    } yield newSenderTrafficState.map(updateTrafficState(ephemeralState, sender, _))

    EitherT(newStateOF.getOrElse(Right(ephemeralState)))
  }

  private def updateTrafficState(
      ephemeralState: BlockUpdateEphemeralState,
      member: Member,
      trafficState: TrafficState,
  ): BlockUpdateEphemeralState =
    ephemeralState
      .focus(_.trafficState)
      .modify(_.updated(member, trafficState))
}

object BlockUpdateGeneratorImpl {

  type SignedEvents = NonEmpty[Map[Member, OrdinarySerializedEvent]]

  type EventsForSubmissionRequest = Map[Member, SequencedEvent[ClosedEnvelope]]

  private final case class SequencedSubmission(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      topologySnapshotO: Option[SyncCryptoApi],
  )(val traceContext: TraceContext)

}
