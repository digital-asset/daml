// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.data.{Chain, EitherT}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.alternative.*
import cats.syntax.either.*
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
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.client.SequencedEventValidator.SigningTimestampVerificationError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, MediatorGroup, Member, PartyId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
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
    maybeLowerSigningTimestampBound: Option[CantonTimestamp],
    rateLimitManager: Option[SequencerRateLimitManager],
    implicitMemberRegistration: Boolean,
    orderingTimeFixMode: OrderingTimeFixMode,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

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
  ): Future[BlockUpdates] = {
    val lastBlockTs = blockState.latestBlock.lastTs
    val lastBlockSafePruning =
      blockState.state.status.safePruningTimestampFor(CantonTimestamp.MaxValue)
    val BlockEvents(height, tracedEvents) = blockEvents

    val chunks = splitAfterTopologyClientEvents(tracedEvents)
    logger.debug(s"Splitting block $height into ${chunks.length} chunks")
    val iter = chunks.iterator

    def go(state: State): Future[BlockUpdates] = {
      if (iter.hasNext) {
        val nextChunk = iter.next()
        processChunk(height, lastBlockTs, lastBlockSafePruning, state, nextChunk).map {
          case (chunkUpdate, nextState) =>
            PartialBlockUpdate(chunkUpdate, go(nextState))
        }
      } else {
        val block = BlockInfo(height, state.lastTs, state.latestTopologyClientTimestamp)
        Future.successful(CompleteBlockUpdate(block))
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
      case Send(_, signedSubmissionRequest) if implicitMemberRegistration =>
        signedSubmissionRequest.content.batch.allRecipients.contains(AllMembersOfDomain)
      case Send(_, signedSubmissionRequest) =>
        signedSubmissionRequest.content.batch.allMembers.contains(topologyClientMember)
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
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[(ChunkUpdate, State)] = {
    val lastTsAndFixedTsChangesF = {
      // With this logic, we assign to the initial non-Send events the same timestamp as for the last
      // block. This means that we will include these events in the ephemeral state of the previous block
      // when we re-read it from the database. But this doesn't matter given that all those events are idempotent.
      MonadUtil.foldLeftM[
        Future,
        (
            CantonTimestamp,
            Seq[
              (
                  CantonTimestamp,
                  Traced[LedgerBlockEvent],
                  Either[SubmissionRequestOutcome, Option[SyncCryptoApi]],
              )
            ],
        ),
        Traced[LedgerBlockEvent],
      ](
        (
          state.lastTs,
          Seq.empty,
        ),
        chunk.forgetNE,
      ) { case ((lastTs, events), event) =>
        event.value match {
          case send: LedgerBlockEvent.Send =>
            val ts =
              ensureStrictlyIncreasingTimestamp(lastTs, send.timestamp)
            logger.info(
              show"Observed Send with messageId ${send.signedSubmissionRequest.content.messageId.singleQuoted} in block $height and assigned it timestamp $ts"
            )(event.traceContext)
            validateTimestampOfSigningKey(
              ts,
              send.signedSubmissionRequest.content,
              state.latestTopologyClientTimestamp,
              state.ephemeral.heads.get(topologyClientMember),
              state.ephemeral.tryNextCounter,
            )(event.traceContext, ec).value.map(signingSnapshotOrError =>
              (ts, events :+ ((ts, event, signingSnapshotOrError)))
            )
          case _ =>
            logger.info(s"Observed ${event.value} in block $height at timestamp $lastTs")(
              event.traceContext
            )
            Future.successful((lastTs, events :+ (lastTs, event, Right(None))))
        }
      }
    }

    for {
      lastTsAndFixedTsChanges <- lastTsAndFixedTsChangesF
      (lastTs, fixedTsChanges) = lastTsAndFixedTsChanges

      membersToDisable = fixedTsChanges.collect { case (_, Traced(DisableMember(member)), _) =>
        member
      }
      submissionRequests = fixedTsChanges.collect {
        case (ts, ev @ Traced(sendEvent: Send), signingSnapshotOrOutcome) =>
          // Discard the timestamp of the `Send` event as this one is obsolete
          (ts, ev.map(_ => sendEvent.signedSubmissionRequest), signingSnapshotOrOutcome)
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
      safePruningTimestamp = lastBlockSafePruning.min(lastBlockTs)
      allPruneRequests = fixedTsChanges.collect { case (_, traced @ Traced(Prune(ts)), _) =>
        Traced(ts)(traced.traceContext)
      }
      (pruneRequests, invalidPruneRequests) = allPruneRequests.partition(
        _.value <= safePruningTimestamp
      )
      _ = if (invalidPruneRequests.nonEmpty) {
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

      newMembers <- detectMembersWithoutSequencerCounters(fixedTsChanges, state)
      _ = if (newMembers.nonEmpty)
        logger.info(
          s"Detected new members without sequencer counter: $newMembers"
        )
      validatedAcks <- processAcknowledgements(lastBlockTs, state, fixedTsChanges)
      (acksByMember, invalidAcks) = validatedAcks
      // Warn if we use an approximate snapshot but only after we've read at least one
      warnIfApproximate = state.ephemeral.heads
        .get(topologyClientMember)
        .exists(_ > SequencerCounter.Genesis)
      newMembersTraffic <-
        OptionUtil.zipWithFDefaultValue(
          rateLimitManager,
          SyncCryptoClient
            .getSnapshotForTimestamp(
              client = domainSyncCryptoApi,
              desiredTimestamp = lastTs,
              previousTimestampO = state.latestTopologyClientTimestamp,
              protocolVersion = protocolVersion,
              warnIfApproximate = warnIfApproximate,
            )
            .flatMap(_.ipsSnapshot.trafficControlParameters(protocolVersion)),
          Map.empty[Member, TrafficState],
        ) { case (rlm, parameters) =>
          newMembers.toList
            .parTraverse { case (member, timestamp) =>
              rlm
                .createNewTrafficStateAt(
                  member,
                  timestamp.immediatePredecessor,
                  parameters,
                )
                .map(member -> _)
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
      result <- MonadUtil
        .foldLeftM(
          (
            Seq.empty[SignedEvents],
            Map.empty[AggregationId, InFlightAggregationUpdate],
            stateWithNewMembers.ephemeral,
          ),
          submissionRequests,
        )(validateSubmissionRequestAndAddEvents(height, state.latestTopologyClientTimestamp))
    } yield result match {
      case (reversedSignedEvents, inFlightAggregationUpdates, finalEphemeralState) =>
        val lastTopologyClientEventTs =
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

  private def detectMembersWithoutSequencerCounters(
      fixedTsChanges: Seq[
        (
            CantonTimestamp,
            Traced[LedgerBlockEvent],
            Either[SubmissionRequestOutcome, Option[SyncCryptoApi]],
        )
      ],
      state: State,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Map[Member, CantonTimestamp]] = {
    if (implicitMemberRegistration) {
      fixedTsChanges
        .parFoldMapA {
          case (_, Traced(Send(_, _)), Left(_)) =>
            // trying to get the signingsnapshot failed previously. since we'll anyway reject the request further down the line,
            // we don't need to detect new members in the recipients
            Future.successful(Seq.empty)

          case (sequencingTimestamp, Traced(Send(_, event)), Right(signingSnapshotO)) =>
            def recipientIsKnown(topologySnapshot: TopologySnapshot, member: Member) = {
              if (!member.isAuthenticated) Future.successful(Nil)
              else
                topologySnapshot
                  .isMemberKnown(member)
                  .map(Option.when(_)(member).toList)
            }

            import event.content.sender
            for {
              topologySnapshot <- SyncCryptoClient
                .getSnapshotForTimestamp(
                  domainSyncCryptoApi,
                  sequencingTimestamp,
                  state.latestTopologyClientTimestamp,
                  protocolVersion,
                  warnIfApproximate = false,
                )
                .map(_.ipsSnapshot)
              signingOrSequencingSnapshot = signingSnapshotO
                .map(_.ipsSnapshot)
                .getOrElse(topologySnapshot)

              groupToMembers <- resolveGroupsToMembers(
                event.content.batch.allRecipients.collect { case groupRecipient: GroupRecipient =>
                  groupRecipient
                },
                signingOrSequencingSnapshot,
              )
              memberRecipients = event.content.batch.allRecipients.collect {
                case MemberRecipient(member) => member
              }
              knownMemberRecipientsOrSender <- (memberRecipients.toSeq :+ sender).parFoldMapA(
                recipientIsKnown(topologySnapshot, _)
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
          case (_, _, _) => Future.successful(Seq.empty)
        }
        .map(
          _.groupBy(_._1)
            .flatMap { case (member, tss) => tss.map(_._2).minOption.map(member -> _) }
        )
    } else {
      Future.successful(fixedTsChanges.collect {
        // to support idempotency we ignore new requests to add already existing members
        case (ts, Traced(AddMember(member)), _)
            if !state.ephemeral.registeredMembers.contains(member) =>
          member -> ts
      }.toMap)
    }
  }

  private def processAcknowledgements(
      lastBlockTs: CantonTimestamp,
      state: State,
      fixedTsChanges: Seq[
        (
            CantonTimestamp,
            Traced[LedgerBlockEvent],
            Either[SubmissionRequestOutcome, Option[SyncCryptoApi]],
        )
      ],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[(Map[Member, CantonTimestamp], Seq[(Member, CantonTimestamp, BaseError)])] = {
    for {
      snapshot <- SyncCryptoClient
        .getSnapshotForTimestamp(
          domainSyncCryptoApi,
          lastBlockTs,
          state.latestTopologyClientTimestamp,
          protocolVersion,
          warnIfApproximate = false,
        )
      allAcknowledgements = fixedTsChanges.collect { case (_, t @ Traced(Acknowledgment(ack)), _) =>
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
      sigChecks <- Future.sequence(goodTsAcks.map(_.withTraceContext {
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
      }.value))
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
      sequencedSubmissionRequest: (
          CantonTimestamp,
          Traced[SignedContent[SubmissionRequest]],
          Either[SubmissionRequestOutcome, Option[SyncCryptoApi]],
      ),
  )(implicit
      ec: ExecutionContext
  ): Future[(Seq[SignedEvents], InFlightAggregationUpdates, EphemeralState)] = {
    val (reversedEvents, inFlightAggregationUpdates, stFromAcc) = acc
    val (sequencingTimestamp, tracedSubmissionRequest, signingSnapshotOrOutcome) =
      sequencedSubmissionRequest
    tracedSubmissionRequest.withTraceContext { implicit traceContext => signedSubmissionRequest =>
      val submissionRequest = signedSubmissionRequest.content

      def processSubmissionOutcome(st: EphemeralState, outcome: SubmissionRequestOutcome): Future[
        (
            Seq[SignedEvents],
            InFlightAggregationUpdates,
            EphemeralState,
        )
      ] = outcome match {
        case SubmissionRequestOutcome(deliverEvents, newAggregationO, signingSnapshotO) =>
          NonEmpty.from(deliverEvents) match {
            case None => // No state update if there is nothing to deliver
              Future.successful(acc)
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
              // If the submission request is invalid, say because the timestamp of signing key is too old,
              // we sign the deliver error with the sequencing timestamp's key even if the submission request
              // specifies a timestamp of signing key.
              val signingTimestamp =
                if (signingSnapshotO.isDefined) submissionRequest.timestampOfSigningKey else None

              // In some cases we use the sequencing timestamp to determine the topology snapshot with the
              // sequencer signing key.
              def getTopologySnapshotForSequencingTimestamp(
                  ts: CantonTimestamp
              ): Future[SyncCryptoApi] = {
                val warnIfApproximate =
                  st.heads.get(topologyClientMember).exists(_ > SequencerCounter.Genesis)
                SyncCryptoClient.getSnapshotForTimestamp(
                  client = domainSyncCryptoApi,
                  desiredTimestamp = ts,
                  previousTimestampO = latestTopologyClientTimestamp,
                  protocolVersion = protocolVersion,
                  warnIfApproximate = warnIfApproximate,
                )
              }
              for {
                signingSnapshot <-
                  signingSnapshotO.fold {
                    // If we haven't yet computed a snapshot for signing,
                    // we now get one for the sequencing timestamp
                    getTopologySnapshotForSequencingTimestamp(sequencingTimestamp)
                  }(Future.successful)
                // Update the traffic status of the recipients before generating the events below.
                // Typically traffic state might change even for recipients if a top up becomes effective at that timestamp
                // Doing this here ensures that the traffic state persisted for the event is correct
                // It's also important to do this here after group -> Set[member] resolution has been performed so we get
                // the actual member recipients
                trafficUpdatedState <- updateTrafficStates(
                  newState,
                  deliverEventsNE.keySet,
                  sequencingTimestamp,
                  signingSnapshot,
                )
                signedEvents <- signEvents(
                  deliverEventsNE,
                  signingSnapshot,
                  signingTimestamp,
                  trafficUpdatedState,
                  sequencingTimestamp,
                  getTopologySnapshotForSequencingTimestamp,
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
          signingSnapshotOrOutcome,
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

  private def ensureSigningSnapshotPresentForAggregationRule(
      submissionRequest: SubmissionRequest,
      signingSnapshotO: Option[SyncCryptoApi],
      sequencingTimestamp: CantonTimestamp,
      st: EphemeralState,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] = {
    EitherTUtil.condUnitET(
      submissionRequest.aggregationRule.isEmpty || signingSnapshotO.isDefined,
      invalidSubmissionRequest(
        submissionRequest,
        sequencingTimestamp,
        SequencerErrors.SigningTimestampMissing(
          "SigningSnapshot is not defined for a submission with an `aggregationRule` present. Please check that `timestampOfSigningKey` has been set for the submission."
        ),
        st.tryNextCounter(submissionRequest.sender),
      ),
    )
  }

  private def validateMaxSequencingTime(
      submissionRequest: SubmissionRequest,
      signingSnapshotO: Option[SyncCryptoApi],
      sequencingTimestamp: CantonTimestamp,
      st: EphemeralState,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] = signingSnapshotO match {
    case None => EitherT.right[SubmissionRequestOutcome](Future.unit)
    case Some(signingSnapshot) =>
      for {
        domainParameters <- EitherT(signingSnapshot.ipsSnapshot.findDynamicDomainParameters())
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
          submissionRequest.aggregationRule.isEmpty
            || submissionRequest.maxSequencingTime.toInstant.isBefore(maxSequencingTimeUpperBound),
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

  /** Returns the snapshot for signing the events (if the submission request specifies a signing timestamp)
    * and the sequenced events by member.
    *
    * Drops the submission request if the sender is not registered or
    * the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.maxSequencingTime]]
    * is before the `sequencingTimestamp`.
    *
    * Produces a [[com.digitalasset.canton.sequencing.protocol.DeliverError]]
    * if some recipients are unknown or the requested
    * [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.timestampOfSigningKey]]
    * is too old or after the `sequencingTime`.
    */
  private def validateAndGenerateSequencedEvents(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      st: EphemeralState,
      signingSnapshotOrOutcome: Either[SubmissionRequestOutcome, Option[SyncCryptoApi]],
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[(EphemeralState, SubmissionRequestOutcome)] = {
    val submissionRequest = signedSubmissionRequest.content

    // In the following EitherT, Lefts are used to stop processing the submission request and immediately produce the sequenced events
    val resultET = for {
      _ <- EitherT.cond[Future](
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
      _ <- EitherT.cond[Future](
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
      _ <- EitherT.fromEither[Future](
        checkRecipientsAreKnown(
          submissionRequest,
          sequencingTimestamp,
          st.registeredMembers,
          st.tryNextCounter,
        )
      )
      _ <- checkSignatureOnSubmissionRequest(
        sequencingTimestamp,
        signedSubmissionRequest,
        latestTopologyClientTimestamp,
      )
      signingSnapshotO <- EitherT.fromEither[Future](signingSnapshotOrOutcome)
      _ <- ensureSigningSnapshotPresentForAggregationRule(
        submissionRequest,
        signingSnapshotO,
        sequencingTimestamp,
        st,
      )
      _ <- validateMaxSequencingTime(
        submissionRequest,
        signingSnapshotO,
        sequencingTimestamp,
        st,
      )
      _ <- checkClosedEnvelopesSignatures(
        signingSnapshotO,
        signedSubmissionRequest.content,
        sequencingTimestamp,
      )
      groupToMembers <- computeGroupAddressesToMembers(
        submissionRequest,
        sequencingTimestamp,
        latestTopologyClientTimestamp,
        signingSnapshotO,
        st,
      )
      stateAfterTrafficConsume <- EitherT.liftF {
        updateRateLimiting(
          submissionRequest,
          sequencingTimestamp,
          latestTopologyClientTimestamp,
          groupToMembers,
          st,
        )
      }
      _ <- EitherT.cond[Future](
        SequencerValidations.checkFromParticipantToAtMostOneMediator(submissionRequest),
        (), {
          SequencerError.MultipleMediatorRecipients
            .Error(submissionRequest, sequencingTimestamp)
            .report()
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      aggregationIdO = submissionRequest.aggregationId(domainSyncCryptoApi.pureCrypto)
      aggregationOutcome <- EitherT.fromEither[Future](
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
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] = {
    val submissionRequest = signedSubmissionRequest.content

    // if we haven't seen any topology transactions yet, then we cannot verify signatures, so we skip it.
    // in practice this should only happen for the first ever transaction, which contains the initial topology data.
    val skipCheck =
      latestTopologyClientTimestamp.isEmpty || !submissionRequest.sender.isAuthenticated
    if (skipCheck) {
      EitherT.pure[Future, SubmissionRequestOutcome](())
    } else {
      val alarmE = for {
        timestampOfSigningKey <- EitherT.fromEither[Future](
          signedSubmissionRequest.timestampOfSigningKey.toRight[BaseAlarm](
            SequencerError.MissingSubmissionRequestSignatureTimestamp.Error(
              signedSubmissionRequest,
              sequencingTimestamp,
            )
          )
        )
        snapshot <- EitherT.right(
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
          SyncCryptoClient.getSnapshotForTimestamp(
            domainSyncCryptoApi,
            sequencingTimestamp,
            latestTopologyClientTimestamp,
            protocolVersion,
            warnIfApproximate = false,
          )
        )
        _ <- signedSubmissionRequest
          .verifySignature(
            snapshot,
            submissionRequest.sender,
            HashPurpose.SubmissionRequestSignature,
          )
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

  private def validateTimestampOfSigningKey(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      topologyClientMemberHead: => Option[SequencerCounter],
      nextCounter: Member => SequencerCounter,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Option[SyncCryptoApi]] =
    submissionRequest.timestampOfSigningKey match {
      case None => EitherT.pure(None)
      case Some(signingTimestamp) =>
        def rejectInvalidTimestampOfSigningKey(
            reason: SigningTimestampVerificationError
        ): SubmissionRequestOutcome = {
          val rejection = reason match {
            case SequencedEventValidator.SigningTimestampAfterSequencingTime =>
              SequencerErrors.SigningTimestampAfterSequencingTimestamp(
                signingTimestamp,
                sequencingTimestamp,
              )
            case SequencedEventValidator.SigningTimestampTooOld(_) |
                SequencedEventValidator.NoDynamicDomainParameters(_) =>
              SequencerErrors.SigningTimestampTooEarly(
                signingTimestamp,
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
          .validateSigningTimestamp(
            domainSyncCryptoApi,
            signingTimestamp,
            sequencingTimestamp,
            latestTopologyClientTimestamp,
            protocolVersion,
            warnIfApproximate,
          )
          .bimap(
            rejectInvalidTimestampOfSigningKey,
            signingSnapshot => Some(signingSnapshot),
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
            refuse(
              SequencerErrors.AggregateSubmissionAlreadySent(message)
            )
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
          s"Aggregation ID $aggregationId has reached its threshold ${newAggregation.rule.threshold} and will be delivered at $sequencingTimestamp"
        ), {
          logger.debug(
            s"Aggregation ID $aggregationId has now ${newAggregation.aggregatedSenders.size} senders aggregated. Threshold is ${newAggregation.rule.threshold.value}"
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

  private def resolveGroupsToMembers(
      groupRecipients: Set[GroupRecipient],
      topologySnapshot: TopologySnapshot,
  )(implicit executionContext: ExecutionContext): Future[Map[GroupRecipient, Set[Member]]] = {
    if (groupRecipients.isEmpty) Future.successful(Map.empty)
    else
      for {
        participantsOfParty <- {
          val parties = groupRecipients.collect { case ParticipantsOfParty(party) =>
            party.toLf
          }
          if (parties.isEmpty)
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              mapping <-
                topologySnapshot
                  .activeParticipantsOfParties(parties.toSeq)
            } yield mapping.map[GroupRecipient, Set[Member]] { case (party, participants) =>
              ParticipantsOfParty(
                PartyId.tryFromLfParty(party)
              ) -> participants.toSet[Member]
            }
        }
        mediatorsOfDomain <- {
          val mediatorGroups = groupRecipients.collect { case MediatorsOfDomain(group) =>
            group
          }.toSeq
          if (mediatorGroups.isEmpty)
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              groups <- topologySnapshot
                .mediatorGroupsOfAll(mediatorGroups)
                .leftMap(_ => Seq.empty[MediatorGroup])
                .merge
            } yield groups
              .map(group =>
                MediatorsOfDomain(group.index) -> (group.active ++ group.passive)
                  .toSet[Member]
              )
              .toMap[GroupRecipient, Set[Member]]
        }
        allRecipients <- {
          if (!groupRecipients.contains(AllMembersOfDomain)) {
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          } else {
            topologySnapshot
              .allMembers()
              .map(members => Map((AllMembersOfDomain: GroupRecipient, members)))
          }
        }

        sequencersOfDomain <- {
          val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
          if (useSequencersOfDomain) {
            for {
              sequencers <-
                topologySnapshot
                  .sequencerGroup()
                  .map(
                    _.map(group => (group.active ++ group.passive).toSet[Member])
                      .getOrElse(Set.empty[Member])
                  )
            } yield Map((SequencersOfDomain: GroupRecipient) -> sequencers)
          } else
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
        }
      } yield participantsOfParty ++ mediatorsOfDomain ++ sequencersOfDomain ++ allRecipients
  }

  private def computeGroupAddressesToMembers(
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      signingSnapshotO: Option[SyncCryptoApi],
      st: EphemeralState,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
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
        topologySnapshot <- EitherT.right[SubmissionRequestOutcome](
          signingSnapshotO
            .fold(
              SyncCryptoClient
                .getSnapshotForTimestamp(
                  domainSyncCryptoApi,
                  sequencingTimestamp,
                  latestTopologyClientTimestamp,
                  protocolVersion,
                  warnIfApproximate = false,
                )
            )(Future.successful)
            .map(_.ipsSnapshot)
        )
        participantsOfParty <- {
          val parties = groupRecipients.collect { case ParticipantsOfParty(party) =>
            party.toLf
          }
          if (parties.isEmpty)
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              _ <- topologySnapshot
                .allHaveActiveParticipants(parties)
                .leftMap(parties =>
                  // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                  refuse(s"The following parties do not have active participants $parties")
                )
              mapping <- EitherT.right[SubmissionRequestOutcome](
                topologySnapshot
                  .activeParticipantsOfParties(parties.toSeq)
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
        }
        mediatorsOfDomain <- {
          val mediatorGroups = groupRecipients.collect { case MediatorsOfDomain(group) =>
            group
          }.toSeq
          if (mediatorGroups.isEmpty)
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              groups <- topologySnapshot
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
                MediatorsOfDomain(group.index) -> (group.active ++ group.passive)
                  .toSet[Member]
              )
              .toMap[GroupRecipient, Set[Member]]
        }
        allRecipients <- {
          if (!groupRecipients.contains(AllMembersOfDomain)) {
            EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
          } else {
            for {
              allMembers <- EitherT.right[SubmissionRequestOutcome](
                topologySnapshot
                  .allMembers()
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
        }

        sequencersOfDomain <- {
          val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
          if (useSequencersOfDomain) {
            for {
              sequencers <- EitherT(
                topologySnapshot
                  .sequencerGroup()
                  .map(
                    _.fold[Either[SubmissionRequestOutcome, Set[Member]]](
                      // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                      Left(refuse("No sequencer group found"))
                    )(group => Right((group.active ++ group.passive).toSet))
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
        }
      } yield participantsOfParty ++ mediatorsOfDomain ++ sequencersOfDomain ++ allRecipients
  }

  private def signEvents(
      events: NonEmpty[Map[Member, SequencedEvent[ClosedEnvelope]]],
      snapshot: SyncCryptoApi,
      signingTimestamp: Option[CantonTimestamp],
      ephemeralState: EphemeralState,
      // sequencingTimestamp and getSnapshotAt used for tombstones when snapshot does not include sequencer signing keys
      sequencingTimestamp: CantonTimestamp,
      getSnapshotAt: CantonTimestamp => Future[SyncCryptoApi],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[SignedEvents] = {
    (if (maybeLowerSigningTimestampBound.forall(snapshot.ipsSnapshot.timestamp >= _)) {
       events.toSeq.toNEF
         .parTraverse { case (member, event) =>
           SignedContent
             .create(
               snapshot.pureCrypto,
               snapshot,
               event,
               signingTimestamp,
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
     } else {
       // As the required topology snapshot timestamp is older than the lower signing timestamp bound, the timestamp
       // of this sequencer's very first topology snapshot, tombstone events. This enables subscriptions to signal to
       // subscribers that this sequencer is not in a position to serve the events behind these sequencer counters.
       // Comparing against the lower signing timestamp bound prevents tombstones in "steady-state" sequencing beyond
       // "soon" after initial sequencer onboarding. See #13609
       events.toSeq.toNEF
         .parTraverse { case (member, event) =>
           logger.info(
             s"Sequencer signing key not available on behalf of ${member.uid.id} at ${event.timestamp} and ${event.counter}. Sequencing tombstone."
           )
           for {
             // sign tombstones using key valid at sequencing timestamp as event timestamp has no signing key and we
             // are not sequencing the event anyway, but the tombstone
             sn <- getSnapshotAt(sequencingTimestamp)
             err = DeliverError.create(
               event.counter,
               sequencingTimestamp, // use sequencing timestamp for tombstone
               domainId,
               MessageId(String73.tryCreate("tombstone")), // dummy message id
               SequencerErrors.PersistTombstone(event.timestamp, event.counter),
               protocolVersion,
             )
             signedContent <- SignedContent
               .create(
                 sn.pureCrypto,
                 sn,
                 err,
                 Some(sn.ipsSnapshot.timestamp),
                 HashPurpose.SequencedEventSignature,
                 protocolVersion,
               )
               .valueOr(syncCryptoError =>
                 ErrorUtil.internalError(
                   new RuntimeException(s"Error signing tombstone deliver error: $syncCryptoError")
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
    logger.debug(s"Rejecting submission request $messageId from $sender with error $sequencerError")
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
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      groupToMembers: Map[GroupRecipient, Set[Member]],
      ephemeralState: EphemeralState,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[EphemeralState] = {
    OptionUtil.zipWithFDefaultValue(
      rateLimitManager,
      SyncCryptoClient
        .getSnapshotForTimestamp(
          domainSyncCryptoApi,
          sequencingTimestamp,
          latestTopologyClientTimestamp,
          protocolVersion,
          warnIfApproximate = false,
        )
        .flatMap(_.ipsSnapshot.trafficControlParameters(protocolVersion)),
      ephemeralState,
    ) { case (rlm, parameters) =>
      val sender = request.sender
      // Get the traffic from the ephemeral state
      ephemeralState.trafficState
        .get(sender)
        .map(Future.successful)
        // If it's not there, see if the member is registered and if so create a new traffic state for it
        .orElse {
          ephemeralState.status.members
            .collectFirst {
              case status if status.member == sender && status.enabled =>
                rlm
                  .createNewTrafficStateAt(
                    status.member,
                    status.registeredAt.immediatePredecessor,
                    parameters,
                  )
            }
        }
        .map { trafficStateF =>
          trafficStateF
            .flatMap {
              case trafficState if sequencingTimestamp <= trafficState.timestamp =>
                logger.warn(
                  s"Trying to consume an event with a sequencing timestamp ($sequencingTimestamp)" +
                    s" <= to the current traffic state timestamp ($trafficState)."
                )
                // Unclear what the best thing to do here. For now let the event through and do not deduct traffic
                Future.successful(ephemeralState)
              case trafficState =>
                rlm
                  // Consume traffic for the sender
                  .consume(
                    request.sender,
                    request.batch,
                    sequencingTimestamp,
                    trafficState,
                    parameters,
                    groupToMembers,
                  )
                  .map { newSenderTrafficState =>
                    ephemeralState
                      .copy(trafficState =
                        ephemeralState.trafficState.updated(sender, newSenderTrafficState)
                      )
                  }
                  .valueOr { case error: SequencerRateLimitError.AboveTrafficLimit =>
                    logger.info(
                      s"Submission from member ${error.member} with traffic state '${error.trafficState
                          .map(_.toString)
                          .getOrElse("empty")}' was above traffic limit. Submission cost: ${error.trafficCost.value}. The message will still be delivered."
                    )
                    updateTrafficState(ephemeralState, sender, error.trafficState)
                  }
            }
        }
        // If there's not trace of this member, log it and let the event through
        .getOrElse {
          logger.warn(
            s"Sender $sender unknown by rate limiter. The message will still be delivered."
          )
          Future.successful(ephemeralState)
        }
    }
  }

  private def updateTrafficState(
      ephemeralState: EphemeralState,
      member: Member,
      trafficStateOpt: Option[TrafficState],
  ): EphemeralState = trafficStateOpt
    .map { trafficState =>
      ephemeralState
        .focus(_.trafficState)
        .modify(_.updated(member, trafficState))
    }
    .getOrElse(ephemeralState)
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

}
