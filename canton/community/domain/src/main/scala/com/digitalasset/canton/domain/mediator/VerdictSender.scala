// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.implicits.toFunctorFilterOps
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoError}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.sequencing.client.{
  SendCallback,
  SendResult,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MediatorsOfDomain,
  MemberRecipient,
  OpenEnvelope,
  ParticipantsOfParty,
  Recipient,
  Recipients,
  SequencerErrors,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, MediatorRef, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[mediator] trait VerdictSender {
  def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit]

  def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
      sendVerdict: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Mediator rejects are important for situations where malformed mediator request or RHMs can have valid state
    * and thus consume resources on the participant side. A prompt rejection will allow to free these resources.
    * RHMs are used in this method to identify the affected participants that may have received them and to whom
    * the rejects will be addressed to.
    */
  def sendReject(
      requestId: RequestId,
      requestO: Option[MediatorRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: Verdict.MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit]
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
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val resultET = for {
      snapshot <- EitherT.right(crypto.ips.awaitSnapshot(requestId.unwrap))
      aggregationRule <- EitherT.right(
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
      sendVerdict <- EitherT.right(shouldSendVerdict(request.mediator, snapshot))
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
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
        SendType.Other,
        Some(requestId.unwrap),
        callback = callback,
        maxSequencingTime = decisionTime,
        aggregationRule = aggregationRule,
      )
    } else {
      logger.info(
        s"Not sending the message batch of size ${batch.envelopes.size} for request $requestId as this mediator is passive in the request's mediator group."
      )
      EitherT.pure[Future, String](())
    }

    EitherTUtil
      .logOnError(sendET, s"Failed to send result to sequencer for request ${requestId.unwrap}")
      .value
      .void
  }

  private[this] def createResults(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Batch[DefaultOpenEnvelope]] =
    for {
      snapshot <- EitherT.right(crypto.awaitSnapshot(requestId.unwrap))
      result <- EitherT.right(
        informeesByParticipantAndWithGroupAddressing(
          request.allInformees.toList,
          snapshot.ipsSnapshot,
        )
      )
      (informeesMap, informeesWithGroupAddressing) = result
      envelopes <- {
        val result = request.createMediatorResult(requestId, verdict, request.allInformees)
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
      mediatorRef: MediatorRef,
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[Boolean] = mediatorRef match {
    case MediatorRef.Single(_) => Future.successful(true)
    case MediatorRef.Group(MediatorsOfDomain(mediatorGroupIndex)) =>
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
      mediatorRef: MediatorRef,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[AggregationRule]] =
    mediatorRef match {
      case MediatorRef.Group(MediatorsOfDomain(index)) =>
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
          activeNE = NonEmpty
            .from(mediatorGroup.active)
            .getOrElse(
              ErrorUtil.invalidState(
                "MediatorGroup is expected to have at least 1 active member at this point"
              )
            )
        } yield {
          Some(AggregationRule(activeNE, mediatorGroup.threshold, protocolVersion))
        }
      case MediatorRef.Single(_) =>
        EitherT.right[String](Future.successful(Option.empty[AggregationRule]))
    }

  override def sendReject(
      requestId: RequestId,
      requestO: Option[MediatorRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: Verdict.MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    // For each view type among the root hash messages,
    // we send one rejection message to all participants, where each participant is in its own recipient group.
    // This ensures that participants do not learn from the list of recipients who else is involved in the transaction.
    // This can happen without a malicious submitter, e.g., if the topology has changed.
    val recipientsByViewType =
      rootHashMessages.groupBy(_.protocolMessage.viewType).mapFilter { rhms =>
        val recipients = rhms
          .flatMap(_.recipients.allRecipients.collect[Recipient] {
            case p @ MemberRecipient(_: ParticipantId) => p
            case participantsOfParty: ParticipantsOfParty => participantsOfParty
          })
          .toSet
        NonEmpty.from(recipients.toSeq)
      }
    if (recipientsByViewType.nonEmpty) {
      for {
        snapshot <- crypto.awaitSnapshot(requestId.unwrap)
        envs <- recipientsByViewType.toSeq
          .parTraverse { case (viewType, flatRecipients) =>
            // This is currently a bit messy. We need to a TransactionResultMessage or TransferXResult whenever possible,
            // because that allows us to easily intercept and change the verdict in tests.
            // However, in some cases, the required information is not available, so we fall back to MalformedMediatorRequestResult.
            // TODO(i11326): Remove unnecessary fields from the result message types, so we can get rid of MalformedMediatorRequestResult and simplify this code.
            val rejection = (viewType match {
              case ViewType.TransactionViewType =>
                requestO match {
                  case Some(request @ InformeeMessage(_, _)) =>
                    request.createMediatorResult(
                      requestId,
                      rejectionReason,
                      Set.empty,
                    )
                  // For other kinds of request, or if the request is unknown, we send a generic result
                  case _ =>
                    MalformedMediatorRequestResult.tryCreate(
                      requestId,
                      crypto.domainId,
                      viewType,
                      rejectionReason,
                      protocolVersion,
                    )
                }

              case ViewType.TransferInViewType =>
                TransferInResult.create(
                  requestId,
                  Set.empty,
                  TargetDomainId(crypto.domainId),
                  rejectionReason,
                  protocolVersion,
                )
              case ViewType.TransferOutViewType =>
                TransferOutResult.create(
                  requestId,
                  Set.empty,
                  SourceDomainId(crypto.domainId),
                  rejectionReason,
                  protocolVersion,
                )
              case _: ViewType =>
                MalformedMediatorRequestResult.tryCreate(
                  requestId,
                  crypto.domainId,
                  viewType,
                  rejectionReason,
                  protocolVersion,
                )
            }): MediatorResult

            val recipients = Recipients.recipientGroups(flatRecipients.map(r => NonEmpty(Set, r)))

            SignedProtocolMessage
              .trySignAndCreate(rejection, snapshot, protocolVersion)
              .map(_ -> recipients)
          }
        batch = Batch.of(protocolVersion, envs *)
        // TODO(i13849): Review the case below: the check in sequencer has to be made stricter (not to allow rhms with inconsistent mediators from other than participant domain nodes)
        mediatorRefO = // we always use RHMs to figure out mediator ref, to address rejections from a correct mediator that participants that received the RHMs expect
          rootHashMessages.headOption // one RHM is enough because sequencer checks that all RHMs specify the same mediator recipient
            .map { rhm =>
              rhm.recipients.allRecipients
                .collectFirst {
                  case MemberRecipient(mediatorId: MediatorId) => MediatorRef(mediatorId)
                  case mediatorsOfDomain: MediatorsOfDomain => MediatorRef(mediatorsOfDomain)
                }
                .getOrElse {
                  ErrorUtil.invalidState(
                    "Root hash messages without a mediator recipient are not expected here (should not have been delivered by the sequencer)"
                  )
                }
            }
        _ <- {
          mediatorRefO match {
            case Some(mediatorRef) =>
              for {
                aggregationRuleO <- groupAggregationRule(
                  snapshot.ipsSnapshot,
                  mediatorRef,
                  protocolVersion,
                )
                  .valueOr(reason =>
                    ErrorUtil.invalidState(
                      s"MediatorReject not sent. Failed to determine group aggregation rule for mediator $mediatorRef due to: $reason"
                    )
                  )
                sendVerdict <- shouldSendVerdict(mediatorRef, snapshot.ipsSnapshot)
              } yield {
                sendResultBatch(
                  requestId,
                  batch,
                  decisionTime,
                  aggregationRule = aggregationRuleO,
                  sendVerdict,
                )
              }
            case None => // if no mediator could be detected from RHMs, participants will also detect this and there's not need to send a reject
              Future.unit
          }
        }
      } yield ()
    } else Future.unit
  }
}
