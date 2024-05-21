// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.implicits.toFunctorFilterOps
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Sends confirmation result messages to the informee participants of a request.
  * The result message contains only one envelope addressed to a given informee participant.
  * If the underlying request has several root hashes and is therefore rejected,
  * the VerdictSender will send several batches (one per root hash).
  */
private[mediator] trait VerdictSender {
  def sendResult(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
      sendVerdict: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Mediator rejects are important for situations where malformed mediator confirmation request or RHMs can have valid state
    * and thus consume resources on the participant side. A prompt rejection will allow to free these resources.
    * RHMs are used in this method to identify the affected participants that may have received them and to whom
    * the rejects will be addressed to.
    */
  def sendReject(
      requestId: RequestId,
      requestO: Option[MediatorConfirmationRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: Verdict.MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

private[mediator] object VerdictSender {
  def apply(
      sequencerSend: SequencerClientSend,
      crypto: DomainSyncCryptoClient,
      mediatorId: MediatorId,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): VerdictSender =
    new DefaultVerdictSender(sequencerSend, crypto, mediatorId, protocolVersion, loggerFactory)
}

private[mediator] class DefaultVerdictSender(
    sequencerSend: SequencerClientSend,
    crypto: DomainSyncCryptoClient,
    mediatorId: MediatorId,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends VerdictSender
    with NamedLogging {
  override def sendResult(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val resultET = for {
      snapshot <- EitherT.right(crypto.ips.awaitSnapshotUS(requestId.unwrap))
      aggregationRule <- EitherT
        .right(
          groupAggregationRule(
            snapshot,
            request.mediator,
            protocolVersion,
          )
            .valueOr(err =>
              ErrorUtil.invalidState(
                s"Mediator rule should not fail at this point, the error was: $err"
              )
            )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      sendVerdict <- EitherT
        .right(shouldSendVerdict(request.mediator, snapshot))
        .mapK(FutureUnlessShutdown.outcomeK)
      batch <- createResults(requestId, request, verdict)
      _ <- EitherT.right[SyncCryptoError](
        sendResultBatch(requestId, batch, decisionTime, aggregationRule, sendVerdict)
      )
    } yield ()

    // we don't want to halt the mediator if an individual send fails or if we're unable to create a batch, so just log
    resultET
      .leftMap(err => logger.warn(s"Failed to create or send result message for $requestId: $err"))
      .value
      .map(_.merge)
  }

  override def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
      sendVerdict: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val callback: SendCallback = {
      case UnlessShutdown.Outcome(SendResult.Success(_)) =>
        logger.debug(s"Sent result for request ${requestId.unwrap}")
      case UnlessShutdown.Outcome(SendResult.Error(error)) =>
        val reason = error.reason
        reason match {
          case SequencerErrors.AggregateSubmissionAlreadySent(_) =>
            logger.info(
              s"Result message was already sent for $requestId: $reason"
            )
          case SequencerErrors.SubmissionRequestRefused(_) =>
            logger.warn(
              s"Result message was refused for $requestId: $reason"
            )
          case _ =>
            logger.error(
              s"Failed to send result message for $requestId: $reason"
            )
        }
      case UnlessShutdown.Outcome(_: SendResult.Timeout) =>
        logger.warn("Sequencing result message timed out.")
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.debug("Sequencing result processing was aborted due to shutdown")
    }

    val sendET = if (sendVerdict) {
      // the result of send request will be logged within the returned future however any error is effectively
      // discarded. Any error logged by the eventual callback will most likely occur after the returned future has
      // completed.
      // we use decision-time for max-sequencing-time as recipients will simply ignore the message if received after
      // that point.
      sequencerSend.sendAsync(
        batch,
        Some(requestId.unwrap),
        callback = callback,
        maxSequencingTime = decisionTime,
        aggregationRule = aggregationRule,
        amplify = true,
      )
    } else {
      logger.info(
        s"Not sending the message batch of size ${batch.envelopes.size} for request $requestId as this mediator is passive in the request's mediator group."
      )
      EitherT.pure[FutureUnlessShutdown, String](())
    }

    EitherTUtil
      .logOnErrorU(sendET, s"Failed to send result to sequencer for request ${requestId.unwrap}")
      .value
      .void
  }

  private[this] def createResults(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      verdict: Verdict,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Batch[DefaultOpenEnvelope]] =
    for {
      snapshot <- EitherT.right(crypto.awaitSnapshotUS(requestId.unwrap))
      result <- EitherT
        .right(
          informeesByParticipantAndWithGroupAddressing(
            request.allInformees.toList,
            snapshot.ipsSnapshot,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      (informeesMap, informeesWithGroupAddressing) = result
      envelopes <- {
        val result = ConfirmationResultMessage.create(
          crypto.domainId,
          request.viewType,
          requestId,
          request.rootHash,
          verdict,
          if (request.informeesArePublic) request.allInformees else Set.empty,
          protocolVersion,
        )
        val recipientSeq =
          informeesMap.keys.toSeq.map(MemberRecipient) ++ informeesWithGroupAddressing.toSeq
            .map(p => ParticipantsOfParty(PartyId.tryFromLfParty(p)))
        val recipients =
          NonEmpty
            .from(recipientSeq.map { (r: Recipient) => NonEmpty(Set, r).toSet })
            .map(Recipients.recipientGroups)
            .getOrElse(
              // Should never happen as the topology (same snapshot) is checked in
              // `ConfirmationResponseProcessor.validateRequest`
              ErrorUtil.invalidState("No active participants for informees")
            )

        SignedProtocolMessage
          .signAndCreate(result, snapshot, protocolVersion)
          .map(signedResult => List(OpenEnvelope(signedResult, recipients)(protocolVersion)))
      }

    } yield Batch(envelopes, protocolVersion)

  private def informeesByParticipantAndWithGroupAddressing(
      informees: List[LfPartyId],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): Future[(Map[ParticipantId, Set[LfPartyId]], Set[LfPartyId])] =
    for {
      partiesWithGroupAddressing <- topologySnapshot.partiesWithGroupAddressing(informees)
      participantsByParty <- topologySnapshot.activeParticipantsOfParties(informees)
    } yield {
      val byParticipant = participantsByParty
        .foldLeft(Map.empty[ParticipantId, Set[LfPartyId]]) { case (acc, (party, participants)) =>
          participants.foldLeft(acc) { case (acc, participant) =>
            val parties = acc.getOrElse(participant, Set.empty) + party
            acc.updated(participant, parties)
          }
        }
        .filter(
          _._2.intersect(partiesWithGroupAddressing).isEmpty
        ) // remove participants that are already addressed by some group address
      (byParticipant, partiesWithGroupAddressing)
    }

  private def shouldSendVerdict(
      mediatorGroup: MediatorGroupRecipient,
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    val mediatorGroupIndex = mediatorGroup.group
    topologySnapshot.mediatorGroup(mediatorGroupIndex).map { groupO =>
      groupO
        .getOrElse(
          // This has been checked in the `validateRequest`
          ErrorUtil.invalidState(
            s"Unexpected absent mediator group $mediatorGroupIndex."
          )
        )
        .active
        .contains(mediatorId)
    }
  }

  private def groupAggregationRule(
      topologySnapshot: TopologySnapshot,
      mediatorGroup: MediatorGroupRecipient,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[AggregationRule]] = {
    val index = mediatorGroup.group
    for {
      mediatorGroup <- EitherT(
        topologySnapshot
          .mediatorGroup(index)
          .map(
            _.toRight(
              s"Mediator group $index does not exist in topology at ${topologySnapshot.timestamp}"
            )
          )
      )
    } yield {
      val activeNE = NonEmpty
        .from(mediatorGroup.active)
        .getOrElse(
          ErrorUtil.invalidState(
            "MediatorGroup is expected to have at least 1 active member at this point"
          )
        )
      // We need aggregation only if the mediator group is truly decentralized
      Option.when(mediatorGroup.threshold.unwrap > 1)(
        AggregationRule(activeNE, mediatorGroup.threshold, protocolVersion)
      )
    }
  }

  override def sendReject(
      requestId: RequestId,
      requestO: Option[MediatorConfirmationRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: Verdict.MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // For each view type among the root hash messages,
    // we send one rejection message to all participants, where each participant is in its own recipient group.
    // This ensures that participants do not learn from the list of recipients who else is involved in the transaction.
    // This can happen without a malicious submitter, e.g., if the topology has changed.
    val recipientsByViewTypeAndRootHash =
      rootHashMessages
        .groupBy(m => m.protocolMessage.viewType -> m.protocolMessage.rootHash)
        .mapFilter { rhms =>
          val recipients = rhms
            .flatMap(_.recipients.allRecipients.collect[Recipient] {
              case p @ MemberRecipient(_: ParticipantId) => p
              case participantsOfParty: ParticipantsOfParty => participantsOfParty
            })
            .toSet

          NonEmpty.from(recipients.toSeq)
        }
    if (recipientsByViewTypeAndRootHash.nonEmpty) {
      for {
        snapshot <- crypto.awaitSnapshotUS(requestId.unwrap)
        envs <- recipientsByViewTypeAndRootHash.toSeq
          .parTraverse { case ((viewType, rootHash), flatRecipients) =>
            val rejection = ConfirmationResultMessage.create(
              crypto.domainId,
              viewType,
              requestId,
              rootHash,
              rejectionReason,
              Set.empty,
              protocolVersion,
            )

            val recipients = Recipients.recipientGroups(flatRecipients.map(r => NonEmpty(Set, r)))

            SignedProtocolMessage
              .trySignAndCreate(rejection, snapshot, protocolVersion)
              .map(_ -> recipients)
          }
        batches = envs.map(Batch.of(protocolVersion, _))
        mediatorGroupO = // we always use RHMs to figure out the mediator group, to address rejections from a correct mediator that participants that received the RHMs expect
          rootHashMessages.headOption // one RHM is enough because sequencer checks that all RHMs specify the same mediator recipient
            .map { rhm =>
              rhm.recipients.allRecipients
                .collectFirst { case mediatorGroupRecipient: MediatorGroupRecipient =>
                  mediatorGroupRecipient
                }
                .getOrElse {
                  ErrorUtil.invalidState(
                    "Root hash messages without a mediator recipient are not expected here (should not have been delivered by the sequencer)"
                  )
                }
            }
        _ <- batches.parTraverse_ { batch =>
          mediatorGroupO.traverse_ {
            // if no mediator could be detected from RHMs, participants will also detect this and there's not need to send a reject
            mediatorGroup =>
              for {
                aggregationRuleO <- groupAggregationRule(
                  snapshot.ipsSnapshot,
                  mediatorGroup,
                  protocolVersion,
                ).mapK(FutureUnlessShutdown.outcomeK)
                  .valueOr(reason =>
                    ErrorUtil.invalidState(
                      s"MediatorReject not sent. Failed to determine group aggregation rule for mediator $mediatorGroup due to: $reason"
                    )
                  )
                sendVerdict <- FutureUnlessShutdown.outcomeF(
                  shouldSendVerdict(mediatorGroup, snapshot.ipsSnapshot)
                )
              } yield {
                sendResultBatch(
                  requestId,
                  batch,
                  decisionTime,
                  aggregationRule = aggregationRuleO,
                  sendVerdict,
                )
              }
          }
        }
      } yield ()
    } else FutureUnlessShutdown.unit
  }
}
