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
import com.daml.error.BaseError
import com.daml.nonempty.catsinstances.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.crypto.{
  DomainSyncCryptoClient,
  HashPurpose,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.LedgerBlockEvent.*
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  BlockUpdateClosureWithHeight,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.EphemeralState
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
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
import com.digitalasset.canton.topology.{DomainId, Member, PartyId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  ErrorUtil,
  MapsUtil,
  MonadUtil,
  OptionUtil,
  SeqUtil,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, checked}
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/** Exposes functions that take the deserialized contents of a block from a blockchain integration
  * and computes the new [[com.digitalasset.canton.domain.block.BlockUpdates]].
  * Used by Ethereum and Fabric integrations.
  *
  * In particular, these functions are responsible for the final timestamp assignment of a given submission request.
  * The timestamp assignment works as follows:
  * 1. an initial timestamp is assigned to the submission request by the sequencer that writes it to the ledger
  * 2. each sequencer that reads the block potentially adapts the previously assigned timestamp
  * deterministically via `ensureStrictlyIncreasingTimestamp`
  * 3. this timestamp is used to compute the [[com.digitalasset.canton.domain.block.BlockUpdates]]
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
class BlockUpdateGenerator(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    topologyClientMember: Member,
    maybeLowerTopologyTimestampBound: Option[CantonTimestamp],
    rateLimitManager: Option[SequencerRateLimitManager],
    orderingTimeFixMode: OrderingTimeFixMode,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val closeContext: CloseContext)
    extends NamedLogging {

  import com.digitalasset.canton.domain.block.BlockUpdateGenerator.*

  def asBlockUpdate(block: RawLedgerBlock)(implicit
      executionContext: ExecutionContext
  ): BlockUpdateClosureWithHeight = {
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
    asBlockUpdate(BlockEvents(block.blockHeight, ledgerBlockEvents))
  }

  def asBlockUpdate(blockContents: BlockEvents)(implicit
      executionContext: ExecutionContext
  ): BlockUpdateClosureWithHeight = {
    implicit val blockTraceContext: TraceContext =
      TraceContext.ofBatch(blockContents.events)(logger)
    BlockUpdateClosureWithHeight(
      blockContents.height,
      processEvents(blockContents),
      blockTraceContext,
    )
  }

  private case class State(
      lastTs: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      ephemeral: EphemeralState,
  )

  private def processEvents(blockEvents: BlockEvents)(
      blockState: BlockEphemeralState
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[BlockUpdates] = {
    val lastBlockTs = blockState.latestBlock.lastTs
    val lastBlockSafePruning =
      blockState.state.status.safePruningTimestampFor(CantonTimestamp.MaxValue)
    val BlockEvents(height, tracedEvents) = blockEvents

    val chunks = splitAfterTopologyClientEvents(tracedEvents)
    logger.debug(s"Splitting block $height into ${chunks.length} chunks")
    val iter = chunks.iterator

    def go(state: State): FutureUnlessShutdown[BlockUpdates] = {
      if (iter.hasNext) {
        val nextChunk = iter.next()
        processChunk(height, lastBlockTs, lastBlockSafePruning, state, nextChunk).map {
          case (chunkUpdate, nextState) =>
            PartialBlockUpdate(chunkUpdate, go(nextState))
        }
      } else {
        val block = BlockInfo(height, state.lastTs, state.latestTopologyClientTimestamp)
        FutureUnlessShutdown.pure(CompleteBlockUpdate(block))
      }
    }

    val initialState = State(
      blockState.latestBlock.lastTs,
      blockState.latestBlock.latestTopologyClientTimestamp,
      blockState.state,
    )
    go(initialState)
  }

  private def splitAfterTopologyClientEvents(
      blockEvents: Seq[Traced[LedgerBlockEvent]]
  ): Seq[NonEmpty[Seq[Traced[LedgerBlockEvent]]]] = {
    def possibleTopologyEvent(event: LedgerBlockEvent): Boolean = event match {
      case Send(_, signedSubmissionRequest) =>
        signedSubmissionRequest.content.batch.allRecipients.contains(AllMembersOfDomain)
      case _ => false
    }

    SeqUtil.splitAfter(blockEvents)(event => possibleTopologyEvent(event.value))
  }

  private def processChunk(
      height: Long,
      lastBlockTs: CantonTimestamp,
      lastBlockSafePruning: CantonTimestamp,
      state: State,
      chunk: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(ChunkUpdate, State)] = {
    val (lastTs, revFixedTsChanges) =
      // With this logic, we assign to the initial non-Send events the same timestamp as for the last
      // block. This means that we will include these events in the ephemeral state of the previous block
      // when we re-read it from the database. But this doesn't matter given that all those events are idempotent.
      chunk.forgetNE.foldLeft[
        (CantonTimestamp, Seq[(CantonTimestamp, Traced[LedgerBlockEvent])]),
      ]((state.lastTs, Seq.empty)) { case ((lastTs, events), event) =>
        event.value match {
          case send: LedgerBlockEvent.Send =>
            val ts = ensureStrictlyIncreasingTimestamp(lastTs, send.timestamp)
            logger.info(
              show"Observed Send with messageId ${send.signedSubmissionRequest.content.messageId.singleQuoted} in block $height and assigned it timestamp $ts"
            )(event.traceContext)
            (ts, (ts, event) +: events)
          case _ =>
            logger.info(s"Observed ${event.value} in block $height at timestamp $lastTs")(
              event.traceContext
            )
            (lastTs, (lastTs, event) +: events)
        }
      }
    val fixedTsChanges: Seq[(CantonTimestamp, Traced[LedgerBlockEvent])] = revFixedTsChanges.reverse

    val membersToDisable = fixedTsChanges.collect { case (_, Traced(DisableMember(member))) =>
      member
    }
    val submissionRequests = fixedTsChanges.collect { case (ts, ev @ Traced(sendEvent: Send)) =>
      // Discard the timestamp of the `Send` event as this one is obsolete
      (ts, ev.map(_ => sendEvent.signedSubmissionRequest))
    }

    /* Pruning requests should only specify pruning timestamps that were safe at the time
     * they were submitted for sequencing. A safe pruning timestamp never becomes unsafe,
     * so it should still be safe. We nevertheless double-check this here and error on unsafe pruning requests.
     * Since the safe pruning timestamp is before or at the last acknowledgement of each enabled member,
     * and both acknowledgements and member enabling/disabling take effect only when they are part of a block,
     * the safe pruning timestamp must be at most the last event of the previous block.
     * If it is later, then the sequencer node that submitted the pruning request is buggy
     * and it is better to crash.
     */
    // TODO(M99) Gracefully deal with buggy sequencer nodes
    val safePruningTimestamp = lastBlockSafePruning.min(lastBlockTs)
    val allPruneRequests = fixedTsChanges.collect { case (_, traced @ Traced(Prune(ts))) =>
      Traced(ts)(traced.traceContext)
    }
    val (pruneRequests, invalidPruneRequests) = allPruneRequests.partition(
      _.value <= safePruningTimestamp
    )
    if (invalidPruneRequests.nonEmpty) {
      invalidPruneRequests.foreach(_.withTraceContext { implicit traceContext => pruneTimestamp =>
        logger.error(
          s"Unsafe pruning request at $pruneTimestamp. The latest safe pruning timestamp is $safePruningTimestamp for block $height"
        )
      })
      val alarm = SequencerError.InvalidPruningRequestOnChain.Error(
        height,
        lastBlockTs,
        lastBlockSafePruning,
        invalidPruneRequests.map(_.value),
      )
      throw alarm.asGrpcError
    }

    for {
      submissionRequestsWithSnapshots <- addSnapshots(
        state.latestTopologyClientTimestamp,
        state.ephemeral.heads.get(topologyClientMember),
        submissionRequests,
      )
      newMembers <- detectMembersWithoutSequencerCounters(submissionRequestsWithSnapshots, state)
      _ = if (newMembers.nonEmpty) {
        logger.info(s"Detected new members without sequencer counter: $newMembers")
      }
      validatedAcks <- processAcknowledgements(lastBlockTs, state, fixedTsChanges)
      (acksByMember, invalidAcks) = validatedAcks
      // Warn if we use an approximate snapshot but only after we've read at least one
      warnIfApproximate = state.ephemeral.heads
        .get(topologyClientMember)
        .exists(_ > SequencerCounter.Genesis)
      newMembersTraffic <-
        OptionUtil.zipWithFDefaultValue(
          rateLimitManager,
          // We are using the snapshot at lastTs for all new members in this chunk rather than their registration times.
          // In theory, a parameter change could have become effective in between, but we deliberately ignore this for now.
          // Moreover, a member is effectively registered when it appears in the topology state with the relevant certificate,
          // but the traffic state here is created only when the member sends or receives the first message.
          SyncCryptoClient
            .getSnapshotForTimestampUS(
              client = domainSyncCryptoApi,
              desiredTimestamp = lastTs,
              previousTimestampO = state.latestTopologyClientTimestamp,
              protocolVersion = protocolVersion,
              warnIfApproximate = warnIfApproximate,
            )
            .flatMap(s =>
              FutureUnlessShutdown.outcomeF(s.ipsSnapshot.trafficControlParameters(protocolVersion))
            ),
          Map.empty[Member, TrafficState],
        ) { case (rlm, parameters) =>
          newMembers.toList
            .parTraverse { case (member, timestamp) =>
              FutureUnlessShutdown.outcomeF(
                rlm
                  .createNewTrafficStateAt(
                    member,
                    timestamp.immediatePredecessor,
                    parameters,
                  )
                  .map(member -> _)
              )
            }
            .map(_.toMap)
        }
      stateWithNewMembers = {
        val newMemberStatus = newMembers.map { case (member, ts) =>
          member -> SequencerMemberStatus(member, ts, None)
        }

        val membersWithDisablements =
          membersToDisable.foldLeft(state.ephemeral.status.membersMap ++ newMemberStatus) {
            case (membersMap, memberToDisable) =>
              membersMap.updated(memberToDisable, membersMap(memberToDisable).copy(enabled = false))
          }

        val newMembersWithDisablementsAndAcknowledgements =
          acksByMember.foldLeft(membersWithDisablements) { case (membersMap, (member, timestamp)) =>
            membersMap
              .get(member)
              .fold {
                logger.debug(
                  s"Ack at $timestamp for $member being ignored because the member has not yet been registered."
                )
                membersMap
              } { memberStatus =>
                membersMap.updated(member, memberStatus.copy(lastAcknowledged = Some(timestamp)))
              }
          }

        state
          .focus(_.ephemeral.status.membersMap)
          .replace(newMembersWithDisablementsAndAcknowledgements)
          .focus(_.ephemeral.trafficState)
          .modify(_ ++ newMembersTraffic)
      }
      result <- MonadUtil.foldLeftM(
        (
          Seq.empty[SignedEvents],
          Map.empty[AggregationId, InFlightAggregationUpdate],
          stateWithNewMembers.ephemeral,
        ),
        submissionRequestsWithSnapshots,
      )(validateSubmissionRequestAndAddEvents(height, state.latestTopologyClientTimestamp))
    } yield result match {
      case (reversedSignedEvents, inFlightAggregationUpdates, finalEphemeralState) =>
        val lastTopologyClientEventTs: Option[CantonTimestamp] =
          reversedSignedEvents.iterator.collectFirst {
            case memberEvents if memberEvents.contains(topologyClientMember) =>
              checked(memberEvents(topologyClientMember)).timestamp
          }
        val chunkUpdate = ChunkUpdate(
          newMembers,
          membersToDisable,
          acksByMember,
          invalidAcks,
          reversedSignedEvents.reverse,
          inFlightAggregationUpdates,
          pruneRequests,
          lastTopologyClientEventTs,
          finalEphemeralState,
        )
        val newState = State(
          lastTs,
          lastTopologyClientEventTs.orElse(state.latestTopologyClientTimestamp),
          finalEphemeralState,
        )
        (chunkUpdate, newState)
    }
  }

  private def addSnapshots(
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      topologyClientMemberCounter: Option[SequencerCounter],
      submissionRequests: Seq[(CantonTimestamp, Traced[SignedContent[SubmissionRequest]])],
  )(implicit executionContext: ExecutionContext): FutureUnlessShutdown[Seq[SequencedSubmission]] = {
    submissionRequests.parTraverse { case (sequencingTimestamp, tracedSubmissionRequest) =>
      tracedSubmissionRequest.withTraceContext { implicit traceContext => submissionRequest =>
        // Warn if we use an approximate snapshot but only after we've read at least one
        val warnIfApproximate = topologyClientMemberCounter.exists(_ > SequencerCounter.Genesis)
        for {
          sequencingSnapshot <- SyncCryptoClient.getSnapshotForTimestampUS(
            domainSyncCryptoApi,
            sequencingTimestamp,
            latestTopologyClientTimestamp,
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
                  latestTopologyClientTimestamp,
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
      lastBlockTs: CantonTimestamp,
      state: State,
      fixedTsChanges: Seq[(CantonTimestamp, Traced[LedgerBlockEvent])],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    (Map[Member, CantonTimestamp], Seq[(Member, CantonTimestamp, BaseError)])
  ] = {
    for {
      snapshot <- SyncCryptoClient.getSnapshotForTimestampUS(
        domainSyncCryptoApi,
        lastBlockTs,
        state.latestTopologyClientTimestamp,
        protocolVersion,
        warnIfApproximate = false,
      )
      allAcknowledgements = fixedTsChanges.collect { case (_, t @ Traced(Acknowledgment(ack))) =>
        t.map(_ => ack)
      }
      (goodTsAcks, futureAcks) = allAcknowledgements.partition { tracedSignedAck =>
        // Intentionally use the previous block's last timestamp
        // such that the criterion does not depend on how the block events are chunked up.
        tracedSignedAck.value.content.timestamp <= lastBlockTs
      }
      invalidTsAcks = futureAcks.map(_.withTraceContext { implicit traceContext => signedAck =>
        val ack = signedAck.content
        val member = ack.member
        val timestamp = ack.timestamp
        val error =
          SequencerError.InvalidAcknowledgementTimestamp.Error(member, timestamp, lastBlockTs)
        (member, timestamp, error: BaseError)
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
            .leftMap(e =>
              (
                ack.member,
                ack.timestamp,
                SequencerError.InvalidAcknowledgementSignature
                  .Error(signedAck, lastBlockTs, e): BaseError,
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

  /** @param latestTopologyClientTimestamp
    * Since each chunk contains at most one event addressed to the sequencer's topology client,
    * (and if so it's the last event), we can treat this timestamp static for the whole chunk and
    * need not update it in the accumulator.
    */
  private def validateSubmissionRequestAndAddEvents(
      height: Long,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(
      acc: (Seq[SignedEvents], InFlightAggregationUpdates, EphemeralState),
      sequencedSubmissionRequest: SequencedSubmission,
  )(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[(Seq[SignedEvents], InFlightAggregationUpdates, EphemeralState)] = {
    val (reversedEvents, inFlightAggregationUpdates, stFromAcc) = acc
    val SequencedSubmission(
      sequencingTimestamp,
      signedSubmissionRequest,
      sequencingSnapshot,
      signingSnapshotO,
    ) = sequencedSubmissionRequest
    implicit val traceContext = sequencedSubmissionRequest.traceContext

    def processSubmissionOutcome(
        st: EphemeralState,
        outcome: SubmissionRequestOutcome,
    ): FutureUnlessShutdown[
      (
          Seq[SignedEvents],
          InFlightAggregationUpdates,
          EphemeralState,
      )
    ] = outcome match {
      case SubmissionRequestOutcome(deliverEvents, newAggregationO, signingSnapshotO) =>
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
              st.copy(inFlightAggregations = newInFlightAggregations)
                .copy(checkpoints = newCheckpoints)

            // If we haven't yet computed a snapshot for signing,
            // we now get one for the sequencing timestamp
            val signingSnapshot = signingSnapshotO.getOrElse(sequencingSnapshot)
            for {
              // Update the traffic status of the recipients before generating the events below.
              // Typically traffic state might change even for recipients if a top up becomes effective at that timestamp
              // Doing this here ensures that the traffic state persisted for the event is correct
              // It's also important to do this here after group -> Set[member] resolution has been performed so we get
              // the actual member recipients
              trafficUpdatedState <- FutureUnlessShutdown.outcomeF(
                updateTrafficStates(
                  newState,
                  deliverEventsNE.keySet,
                  sequencingTimestamp,
                  sequencingSnapshot,
                )
              )
              signedEvents <- signEvents(
                deliverEventsNE,
                signingSnapshot,
                trafficUpdatedState,
                sequencingTimestamp,
                sequencingSnapshot,
              )
            } yield (
              signedEvents +: reversedEvents,
              newInFlightAggregationUpdates,
              trafficUpdatedState,
            )
        }
    }

    for {
      newStateAndOutcome <- validateAndGenerateSequencedEvents(
        sequencingTimestamp,
        signedSubmissionRequest,
        stFromAcc,
        sequencingSnapshot,
        signingSnapshotO,
        latestTopologyClientTimestamp,
      )
      (newState, outcome) = newStateAndOutcome
      result <- processSubmissionOutcome(newState, outcome)
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
              s"last timestamp $lastTs, provided timestamp: $providedTimestamp"
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

  private def ensureTopologyTimestampPresentForAggregationRuleWithSignatures(
      submissionRequest: SubmissionRequest,
      topologySnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
      st: EphemeralState,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    EitherTUtil.condUnitET(
      submissionRequest.aggregationRule.isEmpty || submissionRequest.topologyTimestamp.isDefined ||
        submissionRequest.batch.envelopes.forall(_.signatures.isEmpty),
      invalidSubmissionRequest(
        submissionRequest,
        sequencingTimestamp,
        SequencerErrors.TopologyTimestampMissing(
          "`topologyTimestamp` is not defined for a submission with an `aggregationRule` and signatures on the envelopes present. Please set `topologyTimestamp` for the submission."
        ),
        st.tryNextCounter(submissionRequest.sender),
      ),
    )

  private def validateMaxSequencingTimeForAggregationRule(
      submissionRequest: SubmissionRequest,
      topologySnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
      st: EphemeralState,
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
      st: EphemeralState,
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshotO: Option[SyncCryptoApi],
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[(EphemeralState, SubmissionRequestOutcome)] = {
    val submissionRequest = signedSubmissionRequest.content

    // In the following EitherT, Lefts are used to stop processing the submission request and immediately produce the sequenced events
    val resultET = for {
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
        // the sequencer is beyond the timestamp allowed for sequencing this request so it is silently dropped
        // a correct sender should be monitoring their sequenced events and noticed that the max-sequencing-time has been
        // exceeded and trigger a timeout
        // we don't log this as a warning as it is expected behaviour. within a distributed network, the source of
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
        latestTopologyClientTimestamp,
      )
      _ <- validateTopologyTimestamp(
        sequencingTimestamp,
        submissionRequest,
        latestTopologyClientTimestamp,
        st.heads.get(topologyClientMember),
        st.tryNextCounter,
      )
      topologySnapshot = signingSnapshotO.getOrElse(sequencingSnapshot)
      _ <- ensureTopologyTimestampPresentForAggregationRuleWithSignatures(
        submissionRequest,
        topologySnapshot,
        sequencingTimestamp,
        st,
      ).mapK(FutureUnlessShutdown.outcomeK)
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
      stateAfterTrafficConsume <- EitherT.liftF {
        updateRateLimiting(
          submissionRequest,
          sequencingTimestamp,
          sequencingSnapshot,
          groupToMembers,
          st,
        )
      }
      _ <- EitherT.cond[FutureUnlessShutdown](
        SequencerValidations.checkFromParticipantToAtMostOneMediator(submissionRequest),
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
          val inFlightAggregation = st.inFlightAggregations.get(aggregationId)
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
      val aggregationUpdate = aggregationOutcome.map {
        case (aggregationId, inFlightAggregationUpdate, _) =>
          aggregationId -> inFlightAggregationUpdate
      }
      stateAfterTrafficConsume -> SubmissionRequestOutcome(
        events,
        aggregationUpdate,
        signingSnapshotO,
      )
    }
    resultET.value.map {
      case Left(outcome) => st -> outcome
      case Right(newStateAndOutcome) => newStateAndOutcome
    }
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
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] = {
    val submissionRequest = signedSubmissionRequest.content

    // if we haven't seen any topology transactions yet, then we cannot verify signatures, so we skip it.
    // in practice this should only happen for the first ever transaction, which contains the initial topology data.
    val skipCheck =
      latestTopologyClientTimestamp.isEmpty || !submissionRequest.sender.isAuthenticated
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
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      topologyClientMemberHead: => Option[SequencerCounter],
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

        // Block sequencers keep track of the latest topology client timestamp
        // only after protocol version 3 has been released,
        // so silence the warning if we are running on a higher protocol version
        // or have not delivered anything to the sequencer's topology client.
        val warnIfApproximate = topologyClientMemberHead.exists(_ > SequencerCounter.Genesis)
        SequencedEventValidator
          .validateTopologyTimestampUS(
            domainSyncCryptoApi,
            topologyTimestamp,
            sequencingTimestamp,
            latestTopologyClientTimestamp,
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
        // TODO(#17380) remove logging the full previous state
        logger.debug(
          s"Aggregation ID $aggregationId has reached its threshold ${newAggregation.rule.threshold} and will be delivered at $sequencingTimestamp. Previous state was $inFlightAggregationO."
        ), {
          // TODO(#17380) remove logging the full previous state
          logger.debug(
            s"Aggregation ID $aggregationId has now ${newAggregation.aggregatedSenders.size} senders aggregated. Threshold is ${newAggregation.rule.threshold.value}. Previous state was $inFlightAggregationO."
          )
          SubmissionRequestOutcome(
            deliverReceipt(submissionRequest, sequencingTimestamp, nextCounter),
            Some(aggregationId -> fullInFlightAggregationUpdate),
            None,
          )
        },
      )
    } yield fullInFlightAggregationUpdate
  }

  private val groupAddressResolver = new GroupAddressResolver(domainSyncCryptoApi)

  private def computeGroupAddressesToMembers(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: SyncCryptoApi,
      st: EphemeralState,
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
        mediatorsOfDomain <- {
          val mediatorGroups = groupRecipients.collect { case MediatorsOfDomain(group) =>
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
                MediatorsOfDomain(group.index) -> (group.active.forgetNE ++ group.passive)
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
      } yield participantsOfParty ++ mediatorsOfDomain ++ sequencersOfDomain ++ allRecipients
  }

  private def signEvents(
      events: NonEmpty[Map[Member, SequencedEvent[ClosedEnvelope]]],
      snapshot: SyncCryptoApi,
      ephemeralState: EphemeralState,
      // sequencingTimestamp and sequencingSnapshot used for tombstones when snapshot does not include sequencer signing keys
      sequencingTimestamp: CantonTimestamp,
      sequencingSnapshot: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[SignedEvents] = {
    (if (maybeLowerTopologyTimestampBound.forall(snapshot.ipsSnapshot.timestamp >= _)) {
       FutureUnlessShutdown.outcomeF(
         events.toSeq.toNEF
           .parTraverse { case (member, event) =>
             SignedContent
               .create(
                 snapshot.pureCrypto,
                 snapshot,
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
                 member -> OrdinarySequencedEvent(
                   signedContent,
                   ephemeralState.trafficState.get(member).map(_.toSequencedEventTrafficState),
                 )(traceContext)
               }
           }
       )
     } else {
       // As the required topology snapshot timestamp is older than the lower topology timestamp bound, the timestamp
       // of this sequencer's very first topology snapshot, tombstone events. This enables subscriptions to signal to
       // subscribers that this sequencer is not in a position to serve the events behind these sequencer counters.
       // Comparing against the lower signing timestamp bound prevents tombstones in "steady-state" sequencing beyond
       // "soon" after initial sequencer onboarding. See #13609
       events.toSeq.toNEF.parTraverse { case (member, event) =>
         logger.info(
           s"Sequencer signing key not available on behalf of ${member.uid.id} at ${event.timestamp} and ${event.counter}. Sequencing tombstone."
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
     })
      .map(_.fromNEF.toMap)
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
          .toMsg(sequencerError.cause, correlationId = None)}"
    )
    SubmissionRequestOutcome.reject(
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
  ): EventsForSubmissionRequest = {
    val sender = request.sender
    val receipt = Deliver.create(
      nextCounter(sender),
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
    Map(sender -> receipt)
  }

  private def updateTrafficStates(
      ephemeralState: EphemeralState,
      members: Set[Member],
      sequencingTimestamp: CantonTimestamp,
      snapshot: SyncCryptoApi,
  )(implicit ec: ExecutionContext, tc: TraceContext) = {
    OptionUtil.zipWithFDefaultValue(
      rateLimitManager,
      snapshot.ipsSnapshot.trafficControlParameters(protocolVersion),
      ephemeralState,
    ) { case (rlm, parameters) =>
      rlm
        .updateTrafficStates(
          members
            .flatMap(member => ephemeralState.trafficState.get(member).map(member -> _))
            .toMap,
          sequencingTimestamp,
          parameters,
        )
        .map { trafficStateUpdates =>
          ephemeralState
            .copy(trafficState = ephemeralState.trafficState ++ trafficStateUpdates)
        }
    }
  }

  private def updateRateLimiting(
      request: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencingSnapshot: SyncCryptoApi,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      ephemeralState: EphemeralState,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[EphemeralState] = {
    val newStateOF = for {
      rlm <- OptionT.fromOption[Future](rateLimitManager)
      parameters <- OptionT(
        sequencingSnapshot.ipsSnapshot.trafficControlParameters(protocolVersion)
      )
      sender = request.sender
      // Get the traffic from the ephemeral state
      trafficState <- OptionT
        .fromOption[Future](ephemeralState.trafficState.get(sender))
        .orElse {
          // If it's not there, see if the member is registered and if so create a new traffic state for it
          val statusO = ephemeralState.status.members.find { status =>
            status.member == sender && status.enabled
          }
          OptionT(
            statusO.traverse(status =>
              rlm.createNewTrafficStateAt(
                status.member,
                status.registeredAt.immediatePredecessor,
                parameters,
              )
            )
          )
        }
        .thereafter {
          case Success(None) =>
            // If there's no trace of this member, log it and let the event through
            logger.warn(
              s"Sender $sender unknown by rate limiter. The message will still be delivered."
            )
          case _ =>
        }
      _ <-
        if (sequencingTimestamp <= trafficState.timestamp) {
          logger.warn(
            s"Trying to consume an event with a sequencing timestamp ($sequencingTimestamp)" +
              s" <= to the current traffic state timestamp ($trafficState)."
          )
          OptionT.none[Future, Unit]
        } else OptionT.some[Future](())
      // Consume traffic for the sender
      newSenderTrafficState <- OptionT.liftF(
        rlm
          .consume(
            request.sender,
            request.batch,
            sequencingTimestamp,
            trafficState,
            parameters,
            groupToMembers,
          )
          .valueOr { case error: SequencerRateLimitError.AboveTrafficLimit =>
            logger.info(
              s"Submission from member ${error.member} with traffic state '${error.trafficState.toString}' was above traffic limit. Submission cost: ${error.trafficCost.value}. The message will still be delivered."
            )
            error.trafficState
          }
      )
    } yield updateTrafficState(ephemeralState, sender, newSenderTrafficState)
    FutureUnlessShutdown.outcomeF(newStateOF.getOrElse(ephemeralState))
  }

  private def updateTrafficState(
      ephemeralState: EphemeralState,
      member: Member,
      trafficState: TrafficState,
  ): EphemeralState =
    ephemeralState
      .focus(_.trafficState)
      .modify(_.updated(member, trafficState))
}

object BlockUpdateGenerator {

  type SignedEvents = NonEmpty[Map[Member, OrdinarySerializedEvent]]

  private type EventsForSubmissionRequest = Map[Member, SequencedEvent[ClosedEnvelope]]

  /** Describes the outcome of processing a submission request:
    *
    * @param eventsByMember      The [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]]s
    *                            that should be delivered as a result of the submission request.
    *                            This can be [[com.digitalasset.canton.sequencing.protocol.DeliverError]]s,
    *                            receipts for the sender, or plain [[com.digitalasset.canton.sequencing.protocol.Deliver]].
    * @param inFlightAggregation If [[scala.Some$]], the [[com.digitalasset.canton.sequencing.protocol.AggregationId]]
    *                            and the [[com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregationUpdate]]
    *                            of the current in-flight aggregation state.
    *                            If [[scala.None$]], then the submission request either is not aggregatable or was refused.
    * @param signingSnapshotO    The snapshot to be used for signing the envelopes,
    *                            if it should be different from the snapshot at the sequencing time.
    */
  final case class SubmissionRequestOutcome(
      eventsByMember: EventsForSubmissionRequest,
      inFlightAggregation: Option[(AggregationId, InFlightAggregationUpdate)],
      signingSnapshotO: Option[SyncCryptoApi],
  )

  private object SubmissionRequestOutcome {
    val discardSubmissionRequest: SubmissionRequestOutcome =
      SubmissionRequestOutcome(Map.empty, None, None)

    def reject(sender: Member, rejection: DeliverError): SubmissionRequestOutcome =
      SubmissionRequestOutcome(Map(sender -> rejection), None, None)
  }

  private final case class SequencedSubmission(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      topologySnapshotO: Option[SyncCryptoApi],
  )(val traceContext: TraceContext)

}
