// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.Chain
import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.{Foldable, Monoid}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewType.{
  TransactionViewType,
  TransferInViewType,
  TransferOutViewType,
}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.protocol.transfer.{
  TransferInProcessor,
  TransferOutProcessor,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.ProtocolMessage.select
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestAndRootHashMessage, RequestProcessor, RootHash}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.processing.{
  SequencedTime,
  TopologyTransactionProcessorCommon,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import com.google.rpc.status.Status
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** Dispatches the incoming messages of the [[com.digitalasset.canton.sequencing.client.SequencerClient]]
  * to the different processors. It also informs the [[conflictdetection.RequestTracker]] about the passing of time for messages
  * that are not processed by the [[TransactionProcessor]].
  */
trait MessageDispatcher { this: NamedLogging =>
  import MessageDispatcher.*

  protected def protocolVersion: ProtocolVersion

  protected def domainId: DomainId

  protected def participantId: ParticipantId

  protected type ProcessingResult
  protected def doProcess[A](
      kind: MessageKind[A],
      run: => FutureUnlessShutdown[A],
  ): FutureUnlessShutdown[ProcessingResult]
  protected implicit def processingResultMonoid: Monoid[ProcessingResult]

  protected def requestTracker: RequestTracker

  protected def requestProcessors: RequestProcessors

  protected def topologyProcessor
      : (SequencerCounter, SequencedTime, Traced[List[DefaultOpenEnvelope]]) => HandlerResult
  protected def acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType
  protected def requestCounterAllocator: RequestCounterAllocator
  protected def recordOrderPublisher: RecordOrderPublisher
  protected def badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor
  protected def repairProcessor: RepairProcessor
  protected def inFlightSubmissionTracker: InFlightSubmissionTracker
  protected def metrics: SyncDomainMetrics

  implicit protected val ec: ExecutionContext

  def handleAll(events: Traced[Seq[WithOpeningErrors[PossiblyIgnoredProtocolEvent]]]): HandlerResult

  /** Returns a future that completes when all calls to [[handleAll]]
    * whose returned [[scala.concurrent.Future]] has completed prior to this call have completed processing.
    */
  @VisibleForTesting
  def flush(): Future[Unit]

  private def processAcsCommitmentEnvelope(
      envelopes: List[DefaultOpenEnvelope],
      sc: SequencerCounter,
      ts: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val acsCommitments = envelopes.mapFilter(select[SignedProtocolMessage[AcsCommitment]])
    if (acsCommitments.nonEmpty) {
      // When a participant receives an ACS commitment from a counter-participant, the counter-participant
      // expects to receive the corresponding commitment from the local participant.
      // However, the local participant may not have seen neither an ACS change nor a time proof
      // since the commitment's interval end. So we signal an empty ACS change to the ACS commitment processor
      // at the commitment sequencing time (which is after the interval end for an honest counter-participant)
      // so that this triggers an ACS commitment computation on the local participant if necessary.
      //
      // This ACS commitment may be bundled with a request that may lead to a non-empty ACS change at this timestamp.
      // It is nevertheless OK to schedule the empty ACS change
      // because we use a different tie breaker for the empty ACS commitment.
      // This is also why we must not tick the record order publisher here.
      recordOrderPublisher.scheduleEmptyAcsChangePublication(sc, ts)
      doProcess(
        AcsCommitment, {
          logger.debug(s"Processing ACS commitments for timestamp $ts")
          acsCommitmentProcessor(ts, Traced(acsCommitments))
        },
      )
    } else FutureUnlessShutdown.pure(processingResultMonoid.empty)
  }

  private def tryProtocolProcessor(
      viewType: ViewType
  )(implicit traceContext: TraceContext): RequestProcessor[viewType.type] =
    requestProcessors
      .get(viewType)
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalArgumentException(show"No processor for view type $viewType")
        )
      )

  /** Rules for processing batches of envelopes:
    * <ul>
    *   <li>Identity transactions can be included in any batch of envelopes. They must be processed first.
    *     <br/>
    *     The identity processor ignores replayed or invalid transactions and merely logs an error.
    *   </li>
    *   <li>Acs commitments can be included in any batch of envelopes.
    *     They must be processed before the requests and results to
    *     meet the precondition of [[com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor]]'s `processBatch`
    *     method.
    *   </li>
    *   <li>A [[com.digitalasset.canton.protocol.messages.ConfirmationResult]] message should be sent only by the trusted mediator of the domain.
    *     The mediator should never include further messages with a [[com.digitalasset.canton.protocol.messages.ConfirmationResult]].
    *     So a participant accepts a [[com.digitalasset.canton.protocol.messages.ConfirmationResult]]
    *     only if there are no other messages (except topology transactions and ACS commitments) in the batch.
    *     Otherwise, the participant ignores the [[com.digitalasset.canton.protocol.messages.ConfirmationResult]] and raises an alarm.
    *     <br/>
    *     The same applies to a [[com.digitalasset.canton.protocol.messages.MalformedConfirmationRequestResult]] message
    *     that is triggered by root hash messages.
    *     The mediator uses the [[com.digitalasset.canton.data.ViewType]] from the [[com.digitalasset.canton.protocol.messages.RootHashMessage]],
    *     which the participants also used to choose the processor for the request.
    *     So it suffices to forward the [[com.digitalasset.canton.protocol.messages.MalformedConfirmationRequestResult]]
    *     to the appropriate processor.
    *   </li>
    *   <li>
    *     Request messages originate from untrusted participants.
    *     If the batch contains exactly one [[com.digitalasset.canton.protocol.messages.RootHashMessage]]
    *     that is sent to the participant and the mediator only,
    *     the participant processes only request messages with the same root hash.
    *     If there are no such root hash message or multiple thereof,
    *     the participant does not process the request at all
    *     because the mediator will reject the request as a whole.
    *   </li>
    *   <li>
    *     We do not know the submitting member of a particular submission because such a submission may be sequenced through
    *     an untrusted individual sequencer node (e.g., on a BFT domain). Such a sequencer node could lie about
    *     the actual submitting member. These lies work even with signed submission requests
    *     when an earlier submission request is replayed.
    *     So we cannot rely on honest domain nodes sending their messages only once and instead must
    *     deduplicate replays on the recipient side.
    *   </li>
    * </ul>
    */
  protected def processBatch(
      eventE: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val deliver = eventE.event.content
    val Deliver(sc, ts, _, _, batch) = deliver

    val envelopesWithCorrectDomainId = filterBatchForDomainId(batch, sc, ts)

    // Sanity check the batch
    // we can receive an empty batch if it was for a deliver we sent but were not a recipient
    // or if the event validation has failed on the sequencer for another member's submission
    if (deliver.isReceipt) {
      logger.debug(show"Received the receipt for a previously sent batch:\n$deliver")
    } else if (batch.envelopes.isEmpty) {
      logger.debug(show"Received an empty batch.")
    }
    for {
      identityResult <- processTopologyTransactions(
        sc,
        SequencedTime(ts),
        envelopesWithCorrectDomainId,
      )
      acsCommitmentResult <- processAcsCommitmentEnvelope(envelopesWithCorrectDomainId, sc, ts)
      // Make room for the repair requests that have been inserted before the current timestamp.
      //
      // Some sequenced events do not take this code path (e.g. deliver errors),
      // but repair requests may still be tagged to their timestamp.
      // We therefore wedge all of them in now before we possibly allocate a request counter for the current event.
      // It is safe to not wedge repair requests with the sequenced events they're tagged to
      // because wedging affects only request counter allocation.
      repairProcessorResult <- repairProcessorWedging(ts)
      transactionTransferResult <- processTransactionAndTransferMessages(
        eventE,
        sc,
        ts,
        envelopesWithCorrectDomainId,
      )
    } yield Foldable[List].fold(
      List(
        identityResult,
        acsCommitmentResult,
        repairProcessorResult,
        transactionTransferResult,
      )
    )
  }

  protected def processTopologyTransactions(
      sc: SequencerCounter,
      ts: SequencedTime,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] =
    doProcess(
      TopologyTransaction,
      topologyProcessor(sc, ts, Traced(envelopes)),
    )

  private def processTransactionAndTransferMessages(
      eventE: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      sc: SequencerCounter,
      ts: CantonTimestamp,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    def alarmIfNonEmptySigned(
        kind: MessageKind[?],
        envelopes: Seq[
          OpenEnvelope[SignedProtocolMessage[HasRequestId & SignedProtocolMessageContent]]
        ],
    ): Unit =
      if (envelopes.nonEmpty) {
        val requestIds = envelopes.map(_.protocolMessage.message.requestId)
        alarm(sc, ts, show"Received unexpected $kind for $requestIds").discard
      }

    // Extract the participant relevant messages from the batch. All other messages are ignored.
    val encryptedViews = envelopes.mapFilter(select[EncryptedViewMessage[ViewType]])
    val regularMediatorResults =
      envelopes.mapFilter(select[SignedProtocolMessage[RegularConfirmationResult]])
    val MalformedMediatorConfirmationRequestResults =
      envelopes.mapFilter(select[SignedProtocolMessage[MalformedConfirmationRequestResult]])
    val rootHashMessages =
      envelopes.mapFilter(select[RootHashMessage[SerializedRootHashMessagePayload]])

    (
      encryptedViews,
      rootHashMessages,
      regularMediatorResults,
      MalformedMediatorConfirmationRequestResults,
    ) match {
      // Regular confirmation result
      case (Seq(), Seq(), Seq(msg), Seq()) =>
        val viewType = msg.protocolMessage.message.viewType
        val processor = tryProtocolProcessor(viewType)

        doProcess(ResultKind(viewType), processor.processResult(eventE))

      // Confirmation result indicating a malformed confirmation request
      case (Seq(), Seq(), Seq(), Seq(msg)) =>
        val viewType = msg.protocolMessage.message.viewType
        val processor = tryProtocolProcessor(viewType)

        doProcess(
          MalformedMediatorConfirmationRequestMessage,
          processor.processMalformedMediatorConfirmationRequestResult(ts, sc, eventE),
        )

      case _ =>
        // Alarm about invalid confirmation result messages
        regularMediatorResults.groupBy(_.protocolMessage.message.viewType).foreach {
          case (viewType, messages) => alarmIfNonEmptySigned(ResultKind(viewType), messages)
        }
        alarmIfNonEmptySigned(
          MalformedMediatorConfirmationRequestMessage,
          MalformedMediatorConfirmationRequestResults,
        )

        val containsTopologyTransactionsX = DefaultOpenEnvelopesFilter.containsTopologyX(envelopes)

        val isReceipt = eventE.event.content.messageIdO.isDefined
        processEncryptedViewsAndRootHashMessages(
          encryptedViews = encryptedViews,
          rootHashMessages = rootHashMessages,
          containsTopologyTransactionsX = containsTopologyTransactionsX,
          sc = sc,
          ts = ts,
          isReceipt = isReceipt,
        )
    }
  }

  private def processEncryptedViewsAndRootHashMessages(
      encryptedViews: List[OpenEnvelope[EncryptedViewMessage[ViewType]]],
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      containsTopologyTransactionsX: Boolean,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      isReceipt: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    def withNewRequestCounter(
        body: RequestCounter => FutureUnlessShutdown[ProcessingResult]
    ): FutureUnlessShutdown[ProcessingResult] = {
      requestCounterAllocator.allocateFor(sc) match {
        case Some(rc) => body(rc)
        case None => FutureUnlessShutdown.pure(processingResultMonoid.empty)
      }
    }

    def processRequest(goodRequest: GoodRequest) = {
      withNewRequestCounter { rc =>
        val rootHashMessage: goodRequest.rootHashMessage.type = goodRequest.rootHashMessage
        val viewType: rootHashMessage.viewType.type = rootHashMessage.viewType

        val processor = tryProtocolProcessor(viewType)
        val batch = RequestAndRootHashMessage(
          goodRequest.requestEnvelopes,
          rootHashMessage,
          goodRequest.mediator,
          isReceipt,
        )
        doProcess(
          RequestKind(goodRequest.rootHashMessage.viewType),
          processor.processRequest(ts, rc, sc, batch),
        )
      }
    }

    for {
      checkedRootHashMessagesC <- checkRootHashMessageAndViews(ts, rootHashMessages, encryptedViews)
      _ = checkedRootHashMessagesC.nonaborts.iterator.foreach { alarmMsg =>
        alarm(sc, ts, alarmMsg)
      }
      result <- checkedRootHashMessagesC.toEither match {
        case Right(goodRequest) =>
          if (!containsTopologyTransactionsX)
            processRequest(goodRequest)
          else {
            /* A batch should not contain a request and a topology transaction.
             * Handling of such a batch is done consistently with the case [[ExpectMalformedMediatorConfirmationRequestResult]] below.
             */
            alarm(sc, ts, "Invalid batch containing both a request and topology transaction")
            tickRecordOrderPublisher(sc, ts)
          }

        case Left(DoNotExpectMediatorResult) =>
          if (containsTopologyTransactionsX) {
            // The topology processor will tick the record order publisher at the end of the processing
            doProcess(UnspecifiedMessageKind, FutureUnlessShutdown.pure(()))
          } else
            tickRecordOrderPublisher(sc, ts)
        case Left(ExpectMalformedMediatorConfirmationRequestResult) =>
          // The request is malformed from this participant's and the mediator's point of view if the sequencer is honest.
          // An honest mediator will therefore try to send a `MalformedMediatorConfirmationRequestResult`.
          // We do not really care about the result though and just discard the request.
          tickRecordOrderPublisher(sc, ts)
        case Left(SendMalformedAndExpectMediatorResult(rootHash, mediator, reason)) =>
          // The request is malformed from this participant's point of view, but not necessarily from the mediator's.
          doProcess(
            UnspecifiedMessageKind,
            badRootHashMessagesRequestProcessor.sendRejectionAndTerminate(
              sc,
              ts,
              rootHash,
              mediator,
              LocalReject.MalformedRejects.BadRootHashMessages.Reject(reason, protocolVersion),
            ),
          )
      }
    } yield result
  }

  /** Checks the root hash messages and extracts the views with the correct view type.
    * @return [[com.digitalasset.canton.util.Checked.Abort]] indicates a really malformed request and the appropriate reaction
    */
  private def checkRootHashMessageAndViews(
      ts: CantonTimestamp,
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      encryptedViews: List[OpenEnvelope[EncryptedViewMessage[ViewType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Checked[FailedRootHashMessageCheck, String, GoodRequest]] = {
    def checkedT(
        value: FutureUnlessShutdown[Checked[FailedRootHashMessageCheck, String, GoodRequest]]
    ) = CheckedT(value)
    (for {
      filtered <- CheckedT.fromChecked[FutureUnlessShutdown](
        filterRootHashMessages(rootHashMessages, encryptedViews)
      )
      (rootHashMessagesSentToAMediator, allMediators) = filtered
      result <- (rootHashMessagesSentToAMediator: @unchecked) match {
        case Seq(rootHashMessage, furtherRHMs*) =>
          val mediator: MediatorsOfDomain = allMediators.headOption.getOrElse {
            ErrorUtil.internalError(
              new RuntimeException(
                "The previous checks ensure that there is exactly one mediator ID"
              )
            )
          }

          if (furtherRHMs.isEmpty) {
            val recipientsAreValid: FutureUnlessShutdown[Boolean] =
              RootHashMessageRecipients.recipientsAreValid(
                rootHashMessage.recipients,
                participantId,
                mediator,
                badRootHashMessagesRequestProcessor
                  .participantIsAddressByPartyGroupAddress(
                    ts,
                    _,
                    _,
                  ),
              )
            checkedT(recipientsAreValid.map { recipientsAreValid =>
              if (recipientsAreValid) {
                checkEncryptedViewsForRootHashMessage(
                  encryptedViews,
                  rootHashMessage.protocolMessage,
                  mediator,
                )
              } else {
                // We assume that the participant receives only envelopes of which it is a recipient
                Checked.Abort(
                  ExpectMalformedMediatorConfirmationRequestResult: FailedRootHashMessageCheck,
                  Chain(
                    show"Received root hash message with invalid recipients: ${rootHashMessage.recipients}"
                  ),
                )
              }
            })
          } else {
            // Since all messages in `rootHashMessagesSentToAMediator` are addressed to a mediator
            // and there is at most one mediator recipient for all the envelopes in the batch,
            // all these messages must have been sent to the same mediator.
            // This mediator will therefore reject the request as malformed.
            checkedT(
              FutureUnlessShutdown.pure(
                Checked.Abort(
                  ExpectMalformedMediatorConfirmationRequestResult: FailedRootHashMessageCheck,
                  Chain(
                    show"Multiple root hash messages in batch: $rootHashMessagesSentToAMediator"
                  ),
                )
              )
            )
          }
        case Seq() =>
          // The batch may have contained a message that doesn't require a root hash message, e.g., an ACS commitment
          // So raise an alarm only if there are views
          val alarms =
            if (encryptedViews.nonEmpty) Chain(show"No valid root hash message in batch")
            else Chain.empty
          // The participant hasn't received a root hash message that was sent to the mediator.
          // The mediator sends a MalformedMediatorConfirmationRequest only to the recipients of the root hash messages which it has received.
          // It sends a RegularMediatorResult only to the informee participants
          // and it checks that all the informee participants have received a root hash message.
          checkedT(
            FutureUnlessShutdown.pure(
              Checked.Abort(DoNotExpectMediatorResult: FailedRootHashMessageCheck, alarms)
            )
          )
      }
    } yield result).value
  }

  /** Return only the root hash messages sent to a mediator, along with the set of all mediator recipients */
  private def filterRootHashMessages(
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      encryptedViews: List[OpenEnvelope[EncryptedViewMessage[ViewType]]],
  )(implicit traceContext: TraceContext): Checked[
    FailedRootHashMessageCheck,
    String,
    (List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]], Set[MediatorsOfDomain]),
  ] = {
    def hasMediatorGroupRecipient(recipient: Recipient): Boolean = recipient match {
      case _: MediatorsOfDomain => true
      case _ => false
    }

    val (rootHashMessagesSentToAMediator, rootHashMessagesNotSentToAMediator) =
      rootHashMessages.partition(_.recipients.allRecipients.exists(hasMediatorGroupRecipient))

    // The sequencer checks that participants can send only batches that target at most one mediator.
    // Only participants send batches with encrypted views.
    // So we should find at most one mediator among the recipients of the root hash messages if there are encrypted views.
    // We only look at mediator groups because individual mediators should never be addressed directly
    // as part of the protocol
    val allMediators = rootHashMessagesSentToAMediator.foldLeft(Set.empty[MediatorsOfDomain]) {
      (acc, rhm) =>
        val mediators = {
          rhm.recipients.allRecipients.collect { case mediatorsOfDomain: MediatorsOfDomain =>
            mediatorsOfDomain
          }
        }
        acc.union(mediators)
    }
    if (allMediators.sizeCompare(1) > 0 && encryptedViews.nonEmpty) {
      // TODO(M99) The sequencer should have checked that no participant can send such a request.
      //  Honest nodes of the domain should not send such a request either
      //  (though they do send other batches to several mediators, e.g., topology updates).
      //  So the domain nodes or the sequencer are malicious.
      //  Handle this case of dishonest domain nodes more gracefully.
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"Received batch with encrypted views and root hash messages addressed to multiple mediators: $allMediators"
        )
      )
    }

    val result = (rootHashMessagesSentToAMediator, allMediators)
    if (rootHashMessagesNotSentToAMediator.nonEmpty)
      Checked.continueWithResult(
        show"Received root hash messages that were not sent to a mediator: $rootHashMessagesNotSentToAMediator.",
        result,
      )
    else
      Checked.result(result)
  }

  /** Check that we received encrypted views with the same view type as the root hash message.
    * If there is no such view, return an aborting error; otherwise return those views.
    * Also return non-aborting errors for the other received view types (if any).
    */
  private def checkEncryptedViewsForRootHashMessage(
      encryptedViews: List[OpenEnvelope[EncryptedViewMessage[ViewType]]],
      rootHashMessage: RootHashMessage[SerializedRootHashMessagePayload],
      mediator: MediatorsOfDomain,
  ): Checked[SendMalformedAndExpectMediatorResult, String, GoodRequest] = {
    val viewType: rootHashMessage.viewType.type = rootHashMessage.viewType
    val (badEncryptedViewTypes, goodEncryptedViews) = encryptedViews
      .map(_.traverse { encryptedViewMessage =>
        encryptedViewMessage
          .traverse(_.select(viewType))
          .toRight(encryptedViewMessage.viewType)
      })
      .separate

    val badEncryptedViewsC =
      if (badEncryptedViewTypes.nonEmpty)
        Checked.continue(
          show"Expected view type $viewType, but received view types ${badEncryptedViewTypes.distinct}"
        )
      else Checked.result(())

    val goodEncryptedViewsC = NonEmpty.from(goodEncryptedViews) match {
      case None =>
        // We received a batch with at least one root hash message,
        // but no view with the same view type.
        // Send a best-effort Malformed response.
        // This ensures that the mediator doesn't wait until the participant timeout
        // if the mediator expects a confirmation from this participant.
        Checked.Abort(
          SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediator,
            show"Received no encrypted view message of type $viewType",
          ),
          Chain(show"Received no encrypted view message of type $viewType"),
        )
      case Some(nonemptyEncryptedViews) =>
        Checked.result(GoodRequest(rootHashMessage, mediator)(nonemptyEncryptedViews))
    }

    badEncryptedViewsC.flatMap((_: Unit) => goodEncryptedViewsC)
  }

  protected def repairProcessorWedging(
      upToExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    lazy val future = FutureUnlessShutdown.pure {
      repairProcessor.wedgeRepairRequests(upToExclusive)
    }
    doProcess(UnspecifiedMessageKind, future)
  }

  protected def observeSequencing(
      events: Seq[RawProtocolEvent]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val receipts = events.mapFilter {
      case Deliver(counter, timestamp, _domainId, messageIdO, batch) =>
        // The event was submitted by the current participant iff the message ID is set.
        messageIdO.foreach(_ => recordEventDelivered())
        messageIdO.map(_ -> SequencedSubmission(counter, timestamp))
      case DeliverError(_counter, _timestamp, _domainId, _messageId, _reason) =>
        // `observeDeliverError` takes care of generating a rejection reason if necessary
        None
    }
    // In case of duplicate messageIds, we want to keep the earliest submission, but `toMap` keeps the latest
    val receiptsMap = receipts.foldLeft(Map.empty[MessageId, SequencedSubmission]) {
      case (map, (msgId, submission)) if map.contains(msgId) =>
        logger.warn(
          s"Ignoring duplicate $submission for messageId $msgId when observing sequencing"
        )
        map
      case (map, receipt) => map + receipt
    }
    lazy val future =
      FutureUnlessShutdown.outcomeF(
        inFlightSubmissionTracker.observeSequencing(domainId, receiptsMap)
      )
    doProcess(DeliveryMessageKind, future)
  }

  protected def observeDeliverError(
      error: DeliverError
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    recordTrafficDeliveryError(error)
    doProcess(
      DeliveryMessageKind,
      FutureUnlessShutdown.outcomeF(inFlightSubmissionTracker.observeDeliverError(error)),
    )
  }

  private def recordTrafficDeliveryError(error: DeliverError): Unit = error match {
    case DeliverError(_, _, _, _, SequencerErrors.TrafficCredit(_)) =>
      metrics.trafficControl.eventAboveTrafficLimit.mark()
    case _ =>
  }

  private def recordEventDelivered(): Unit = {
    metrics.trafficControl.eventDelivered.mark()
  }

  private def tickRecordOrderPublisher(sc: SequencerCounter, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ProcessingResult] = {
    lazy val future = FutureUnlessShutdown.pure {
      recordOrderPublisher.tick(sc, ts)
    }
    doProcess(UnspecifiedMessageKind, future)
  }

  protected def filterBatchForDomainId(
      batch: Batch[DefaultOpenEnvelope],
      sc: SequencerCounter,
      ts: CantonTimestamp,
  )(implicit traceContext: TraceContext): List[DefaultOpenEnvelope] =
    ProtocolMessage.filterDomainsEnvelopes(
      batch,
      domainId,
      (wrongMsgs: List[DefaultOpenEnvelope]) => {
        alarm(
          sc,
          ts,
          s"Received messages with wrong domain IDs ${wrongMsgs.map(_.protocolMessage.domainId)}. Discarding them.",
        ).discard
        ()
      },
    )

  protected def alarm(sc: SequencerCounter, ts: CantonTimestamp, msg: String)(implicit
      traceContext: TraceContext
  ): Unit = SyncServiceAlarm.Warn(s"(sequencer counter: $sc, timestamp: $ts): $msg").report()

  protected def logTimeProof(sc: SequencerCounter, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      show"Processing time-proof at sc=${sc}, ts=${ts}"
    )
  }

  private def withMsgId(msgId: Option[MessageId]): String = msgId match {
    case Some(id) => s", messageId=$id"
    case None => ""
  }

  protected def logFaultyEvent(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      msgId: Option[MessageId],
      err: WithOpeningErrors[SequencedEvent[DefaultOpenEnvelope]],
  )(implicit traceContext: TraceContext): Unit =
    logger.info(
      show"Skipping faulty event at sc=${sc}, ts=${ts}${withMsgId(msgId)}, with errors=${err.openingErrors
          .map(_.message)} and contents=${err.event.envelopes
          .map(_.protocolMessage)}"
    )

  protected def logEvent(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      msgId: Option[MessageId],
      evt: SignedContent[SequencedEvent[DefaultOpenEnvelope]],
  )(implicit traceContext: TraceContext): Unit = logger.info(
    show"Processing event at sc=${sc}, ts=${ts}${withMsgId(msgId)}, with contents=${evt.content.envelopes
        .map(_.protocolMessage)}"
  )

  protected def logDeliveryError(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      msgId: MessageId,
      status: Status,
  )(implicit traceContext: TraceContext): Unit = logger.info(
    show"Processing delivery error at sc=${sc}, ts=${ts}, messageId=$msgId, status=$status"
  )

}

private[participant] object MessageDispatcher {

  trait RequestProcessors {
    /* A bit of a round-about way to make the Scala compiler recognize that pattern matches on `viewType` refine
     * the type Processor.
     */
    protected def getInternal[P](viewType: ViewType { type Processor = P }): Option[P]
    def get(viewType: ViewType): Option[viewType.Processor] =
      getInternal[viewType.Processor](viewType)
  }

  /** Sigma type to tie the envelope's view type to the root hash message's. */
  private sealed trait GoodRequest {
    val rootHashMessage: RootHashMessage[SerializedRootHashMessagePayload]
    val mediator: MediatorsOfDomain
    val requestEnvelopes: NonEmpty[Seq[
      OpenEnvelope[EncryptedViewMessage[rootHashMessage.viewType.type]]
    ]]
  }
  private object GoodRequest {
    def apply(
        rhm: RootHashMessage[SerializedRootHashMessagePayload],
        mediatorGroup: MediatorsOfDomain,
    )(
        envelopes: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[rhm.viewType.type]]]]
    ): GoodRequest = new GoodRequest {
      override val rootHashMessage: rhm.type = rhm
      override val mediator: MediatorsOfDomain = mediatorGroup
      override val requestEnvelopes
          : NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[rootHashMessage.viewType.type]]]] =
        envelopes
    }
  }

  /** @tparam A The type returned by processors for the given kind
    */
  sealed trait MessageKind[A] extends Product with Serializable with PrettyPrinting {
    override def pretty: Pretty[MessageKind.this.type] = prettyOfObject[MessageKind.this.type]
  }

  case object TopologyTransaction extends MessageKind[AsyncResult]
  final case class RequestKind(viewType: ViewType) extends MessageKind[AsyncResult] {
    override def pretty: Pretty[RequestKind] = prettyOfParam(unnamedParam(_.viewType))
  }
  final case class ResultKind(viewType: ViewType) extends MessageKind[AsyncResult] {
    override def pretty: Pretty[ResultKind] = prettyOfParam(unnamedParam(_.viewType))
  }
  case object AcsCommitment extends MessageKind[Unit]
  case object MalformedMediatorConfirmationRequestMessage extends MessageKind[AsyncResult]
  case object MalformedMessage extends MessageKind[Unit]
  case object UnspecifiedMessageKind extends MessageKind[Unit]
  case object CausalityMessageKind extends MessageKind[Unit]
  case object DeliveryMessageKind extends MessageKind[Unit]

  @VisibleForTesting
  private[protocol] sealed trait FailedRootHashMessageCheck extends Product with Serializable
  @VisibleForTesting
  private[protocol] case object ExpectMalformedMediatorConfirmationRequestResult
      extends FailedRootHashMessageCheck
  @VisibleForTesting
  private[protocol] final case class SendMalformedAndExpectMediatorResult(
      rootHash: RootHash,
      mediator: MediatorsOfDomain,
      rejectionReason: String,
  ) extends FailedRootHashMessageCheck
  @VisibleForTesting
  private[protocol] case object DoNotExpectMediatorResult extends FailedRootHashMessageCheck

  trait Factory[+T <: MessageDispatcher] {
    def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        requestProcessors: RequestProcessors,
        topologyProcessor: (
            SequencerCounter,
            SequencedTime,
            Traced[List[DefaultOpenEnvelope]],
        ) => HandlerResult,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
        metrics: SyncDomainMetrics,
    )(implicit ec: ExecutionContext, tracer: Tracer): T

    def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        transactionProcessor: TransactionProcessor,
        transferOutProcessor: TransferOutProcessor,
        transferInProcessor: TransferInProcessor,
        registerTopologyTransactionResponseProcessor: EnvelopeHandler,
        topologyProcessor: TopologyTransactionProcessorCommon,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
        metrics: SyncDomainMetrics,
    )(implicit ec: ExecutionContext, tracer: Tracer): T = {
      val requestProcessors = new RequestProcessors {
        override def getInternal[P](viewType: ViewType { type Processor = P }): Option[P] =
          viewType match {
            case TransferInViewType => Some(transferInProcessor)
            case TransferOutViewType => Some(transferOutProcessor)
            case TransactionViewType => Some(transactionProcessor)
            case _ => None
          }
      }

      val identityProcessor: (
          SequencerCounter,
          SequencedTime,
          Traced[List[DefaultOpenEnvelope]],
      ) => HandlerResult =
        (counter, timestamp, envelopes) => {
          val registerF = registerTopologyTransactionResponseProcessor(envelopes)
          val processingF = topologyProcessor.processEnvelopes(counter, timestamp, envelopes)
          for {
            r1 <- registerF
            r2 <- processingF
          } yield AsyncResult.monoidAsyncResult.combine(r1, r2)
        }

      create(
        protocolVersion,
        domainId,
        participantId,
        requestTracker,
        requestProcessors,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
        metrics,
      )
    }
  }

  object DefaultFactory extends Factory[MessageDispatcher] {
    override def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        requestProcessors: RequestProcessors,
        topologyProcessor: (
            SequencerCounter,
            SequencedTime,
            Traced[List[DefaultOpenEnvelope]],
        ) => HandlerResult,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
        metrics: SyncDomainMetrics,
    )(implicit ec: ExecutionContext, tracer: Tracer): MessageDispatcher = {
      new DefaultMessageDispatcher(
        protocolVersion,
        domainId,
        participantId,
        requestTracker,
        requestProcessors,
        topologyProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
        metrics,
      )
    }
  }
}
