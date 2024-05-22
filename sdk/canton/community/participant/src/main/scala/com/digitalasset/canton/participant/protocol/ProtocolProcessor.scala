// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, NonEmptyChain}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  Signature,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.RequestOutcome
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  CleanReplayData,
  PendingRequestData,
  ReplayDataOr,
  Wrapped,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.TimeoutResult
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessSet,
  CommitSet,
  RequestTracker,
}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.validation.RecipientsValidator
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{AsyncResult, HandlerResult}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success}

/** The [[ProtocolProcessor]] orchestrates Phase 3, 4, and 7 of the synchronization protocol.
  * For this, it combines [[ProcessingSteps]] specific to a particular kind of request (transaction / transfer)
  * with the common processing steps.
  *
  * @param steps The specific processing steps
  * @tparam SubmissionParam  The bundled submission parameters
  * @tparam SubmissionResult The bundled submission results
  * @tparam RequestViewType     The type of view trees used by the request
  * @tparam SubmissionError  The type of errors that occur during submission processing
  */
abstract class ProtocolProcessor[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    SubmissionError <: WrapsProcessorError,
](
    private[protocol] val steps: ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      SubmissionError,
    ],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClientSend,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit
    ec: ExecutionContext
) extends AbstractMessageProcessor(
      ephemeral,
      crypto,
      sequencerClient,
      protocolVersion,
    )
    with RequestProcessor[RequestViewType] {

  import ProtocolProcessor.*
  import com.digitalasset.canton.util.ShowUtil.*

  def testingConfig: TestingConfigInternal

  def participantId: ParticipantId

  private val recipientsValidator
      : RecipientsValidator[(WithRecipients[steps.DecryptedView], Option[Signature])] =
    new RecipientsValidator(_._1.unwrap, _._1.recipients, loggerFactory)

  private[this] def withKind(message: String): String = s"${steps.requestKind}: $message"

  /** Stores a counter for the submissions.
    * Incremented whenever we pick a mediator for a submission
    * so that we use mediators round-robin.
    *
    * Every processor picks the mediators independently,
    * so it may be that the participant picks the same mediator several times in a row,
    * but for different kinds of requests.
    */
  private val submissionCounter: AtomicInteger = new AtomicInteger(0)

  /** Submits the request to the sequencer, using a recent topology snapshot and the current persisted state
    * as an approximation to the future state at the assigned request timestamp.
    *
    * @param submissionParam The bundled submission parameters
    * @return The submission error or a future with the submission result.
    *         With submission tracking, the outer future completes after the submission is registered as in-flight,
    *         and the inner future after the submission has been sequenced or if it will never be sequenced.
    *         Without submission tracking, both futures complete after the submission has been sequenced
    *         or if it will not be sequenced.
    */
  def submit(submissionParam: SubmissionParam)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    logger.debug(withKind(s"Preparing request ${steps.submissionDescription(submissionParam)}"))

    val recentSnapshot = crypto.currentSnapshotApproximation
    for {
      mediator <- chooseMediator(recentSnapshot.ipsSnapshot)
        .leftMap(steps.embedNoMediatorError)
        .mapK(FutureUnlessShutdown.outcomeK)
      submission <- steps.createSubmission(submissionParam, mediator, ephemeral, recentSnapshot)
      _ = logger.debug(
        s"Topology snapshot timestamp at submission: ${recentSnapshot.ipsSnapshot.timestamp}"
      )
      result <- {
        submission match {
          case untracked: steps.UntrackedSubmission =>
            submitWithoutTracking(submissionParam, untracked).tapLeft(submissionError =>
              logger.warn(s"Failed to submit submission due to $submissionError")
            )
          case tracked: steps.TrackedSubmission => submitWithTracking(submissionParam, tracked)
        }
      }
    } yield result
  }

  private def chooseMediator(
      recentSnapshot: TopologySnapshot
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, NoMediatorError, MediatorGroupRecipient] = {
    val fut = for {
      allMediatorGroups <- recentSnapshot.mediatorGroups()
      allActiveMediatorGroups = allMediatorGroups.filter(_.isActive)
    } yield {
      val mediatorCount = allActiveMediatorGroups.size
      if (mediatorCount == 0) {
        Left(NoMediatorError(recentSnapshot.timestamp))
      } else {
        // Pick the next by incrementing the counter and selecting the mediator modulo the number of all mediators.
        // When the number of mediators changes, this strategy may result in the same mediator being picked twice in a row.
        // This is acceptable as mediator changes are rare.
        //
        // This selection strategy assumes that the `mediators` method in the `MediatorDomainStateClient`
        // returns the mediators in a consistent order. This assumption holds mostly because the cache
        // usually returns the fixed `Seq` in the cache.
        val newSubmissionCounter = submissionCounter.incrementAndGet()
        val chosenIndex = {
          val mod = newSubmissionCounter % mediatorCount
          // The submissionCounter overflows after Int.MAX_VALUE submissions
          // and then the modulo is negative. We must ensure that it's positive!
          if (mod < 0) mod + mediatorCount else mod
        }
        val chosen = checked(allActiveMediatorGroups(chosenIndex)).index
        logger.debug(s"Chose the mediator group $chosen")
        Right(MediatorGroupRecipient(chosen))
      }
    }
    EitherT(fut)
  }

  /** Submits the batch without registering as in-flight and reports send errors as [[scala.Left$]] */
  private def submitWithoutTracking(
      submissionParam: SubmissionParam,
      untracked: steps.UntrackedSubmission,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val result = for {

      maxSequencingTime <- EitherT
        .right(
          untracked.maxSequencingTimeO
            .getOrElse(sequencerClient.generateMaxSequencingTime)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      sendResultAndResultArgs <- submitInternal(
        submissionParam,
        untracked.pendingSubmissionId,
        MessageId.randomMessageId(),
        untracked.batch,
        maxSequencingTime,
        untracked.embedSubmissionError,
      )
      (sendResult, resultArgs) = sendResultAndResultArgs
      result <- EitherT.fromEither[FutureUnlessShutdown](sendResult match {
        case SendResult.Success(deliver) => Right(steps.createSubmissionResult(deliver, resultArgs))
        case SendResult.Error(error) =>
          Left(untracked.embedSubmissionError(SequencerDeliverError(error)))
        case SendResult.Timeout(sequencerTime) =>
          Left(untracked.embedSubmissionError(SequencerTimeoutError(sequencerTime)))
      })
    } yield result
    result.bimap(untracked.toSubmissionError, FutureUnlessShutdown.pure)
  }

  /** Register the submission at the [[submission.InFlightSubmissionTracker]] as in-flight, deduplicate it, and submit it.
    * Errors after the registration are reported asynchronously only and return a [[scala.Right$]].
    * This ensures that every submission generates at most one rejection reason, namely through the
    * timely rejection mechanism. In-flight tracking may concurrently remove the submission at any time
    * and publish the timely rejection event instead of the actual error.
    */
  private def submitWithTracking(
      submissionParam: SubmissionParam,
      tracked: steps.TrackedSubmission,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val maxSequencingTimeF =
      tracked.maxSequencingTimeO
        .mapK(FutureUnlessShutdown.outcomeK)
        .getOrElse(sequencerClient.generateMaxSequencingTime)

    EitherT.right(maxSequencingTimeF).flatMap(submitWithTracking(submissionParam, tracked, _))
  }

  private def submitWithTracking(
      submissionParam: SubmissionParam,
      tracked: steps.TrackedSubmission,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, FutureUnlessShutdown[SubmissionResult]] = {
    val messageUuid = UUID.randomUUID()
    val inFlightSubmission = InFlightSubmission(
      changeIdHash = tracked.changeIdHash,
      submissionId = tracked.submissionId,
      submissionDomain = domainId,
      messageUuid = messageUuid,
      rootHashO = None,
      sequencingInfo =
        UnsequencedSubmission(maxSequencingTime, tracked.submissionTimeoutTrackingData),
      submissionTraceContext = traceContext,
    )
    val messageId = inFlightSubmission.messageId
    val specifiedDeduplicationPeriod = tracked.specifiedDeduplicationPeriod
    logger.debug(s"Registering the submission as in-flight")

    val registeredF = inFlightSubmissionTracker
      .register(inFlightSubmission, specifiedDeduplicationPeriod)
      .leftMap(tracked.embedInFlightSubmissionTrackerError)
      .onShutdown {
        // If we abort due to a shutdown, we don't know whether the submission was registered.
        // The SyncDomain should guard this method call with a performUnlessClosing,
        // so we should see a shutdown here only if the SyncDomain close timeout was exceeded.
        // Therefore, WARN makes sense as a logging level.
        logger.warn(s"Shutdown while registering the submission as in-flight.")
        Left(tracked.shutdownDuringInFlightRegistration)
      }

    def observeSubmissionError(
        newTrackingData: SubmissionTrackingData
    ): Future[SubmissionResult] = {
      // Assign the currently observed domain timestamp so that the error will be published soon.
      // Cap it by the max sequencing time so that the timeout field can move only backwards.
      val timestamp = ephemeral.observedTimestampLookup.highWatermark min maxSequencingTime
      val newUnsequencedSubmission = UnsequencedSubmission(timestamp, newTrackingData)
      for {
        _unit <- inFlightSubmissionTracker.observeSubmissionError(
          tracked.changeIdHash,
          domainId,
          messageId,
          newUnsequencedSubmission,
        )
        // The new timestamp is the sequencing timestamp of the most recently received sequencer message.
        // If this message has already been fully processed and triggered the timely rejections
        // before we updated the UnsequencedSubmission,
        // then the rejection will be emitted only upon the next sequencer message that triggers such a timely rejection.
        // However, it may be an arbitrary long time until this happens.
        // Therefore, we notify the in-flight submission tracker again
        // if it had already been notified for the chosen timestamp or a later one.
        // This should happen only if the domain is idle and no messages are in flight between time observation
        // and notification of the in-flight submission tracker (via the clean sequencer counter tracking).
        // Because the domain is idle, another DB access does not hurt much.
        //
        // There is no point in notifying the in-flight submission tracker if we did not change the timestamp,
        // because the regular timely rejection mechanism has already emitted the command timeout
        // or ongoing processing of the message that triggers the timeout will anyway pick up the old or the updated
        // tracking data.
        _ = if (maxSequencingTime > timestamp)
          ephemeral.timelyRejectNotifier.notifyIfInPastAsync(timestamp)
      } yield tracked.onFailure
    }

    // After in-flight registration, Make sure that all errors get a chance to update the tracking data and
    // instead return a `SubmissionResult` so that the submission will be acknowledged over the ledger API.
    def unlessError[A](eitherT: EitherT[FutureUnlessShutdown, SubmissionTrackingData, A])(
        continuation: A => FutureUnlessShutdown[SubmissionResult]
    ): FutureUnlessShutdown[SubmissionResult] = {
      eitherT.value.transformWith {
        case Success(UnlessShutdown.Outcome(Right(a))) => continuation(a)
        case Success(UnlessShutdown.Outcome(Left(newTrackingData))) =>
          FutureUnlessShutdown.outcomeF(
            observeSubmissionError(newTrackingData)
          )
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          logger.debug(s"Failed to process submission due to shutdown")
          FutureUnlessShutdown.pure(tracked.onFailure)
        case Failure(exception) =>
          // We merely log an error and rely on the maxSequencingTimeout to produce a rejection event eventually.
          // It is not clear whether we managed to send the submission.
          logger.error(s"Failed to submit submission", exception)
          FutureUnlessShutdown.pure(tracked.onFailure)
      }
    }

    def afterRegistration(
        deduplicationResult: Either[DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
    ): FutureUnlessShutdown[SubmissionResult] = deduplicationResult match {
      case Left(failed) =>
        FutureUnlessShutdown.outcomeF(
          observeSubmissionError(tracked.commandDeduplicationFailure(failed))
        )
      case Right(actualDeduplicationOffset) =>
        def sendBatch(
            preparedBatch: steps.PreparedBatch
        ): FutureUnlessShutdown[SubmissionResult] = {
          val submittedEF = submitInternal(
            submissionParam,
            preparedBatch.pendingSubmissionId,
            messageId,
            preparedBatch.batch,
            maxSequencingTime,
            preparedBatch.embedSequencerRequestError,
          ).leftMap { submissionError =>
            logger.warn(s"Failed to submit submission due to $submissionError")
            preparedBatch.submissionErrorTrackingData(submissionError)
          }

          // As the `SendTracker` does not persist its state,
          // we would observe the sequencing here only if the participant has not crashed.
          // We therefore delegate observing the sequencing to the MessageDispatcher,
          // which can rely on the SequencedEventStore for persistence.
          unlessError(submittedEF) { case (sendResult, resultArgs) =>
            val submissionResult = sendResult match {
              case SendResult.Success(deliver) =>
                steps.createSubmissionResult(deliver, resultArgs)
              case _: SendResult.NotSequenced => tracked.onFailure
            }
            FutureUnlessShutdown.pure(submissionResult)
          }
        }

        // There's no point to attempt to send the submission to the sequencer
        // if we've already observed the max sequencing time or something later
        // Rather, we notify the timely rejection mechanism so that the timeout completion
        // is emitted. This should only happen if the max sequencing time was observed
        // after the high watermark check in the InFlightSubmissionTracker.
        val maxSequencingTimeHasElapsed =
          ephemeral.timelyRejectNotifier.notifyIfInPastAsync(maxSequencingTime)
        if (maxSequencingTimeHasElapsed) {
          FutureUnlessShutdown.pure(tracked.onFailure)
        } else {
          val batchF = for {
            batch <- tracked.prepareBatch(
              actualDeduplicationOffset,
              maxSequencingTime,
              ephemeral.sessionKeyStore,
            )
            _ <- EitherT
              .right[SubmissionTrackingData](
                inFlightSubmissionTracker.updateRegistration(inFlightSubmission, batch.rootHash)
              )
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield batch
          unlessError(batchF)(sendBatch)
        }
    }

    registeredF.mapK(FutureUnlessShutdown.outcomeK).map(afterRegistration)
  }

  /** Submit the batch to the sequencer.
    * Also registers `submissionParam` as pending submission.
    */
  private def submitInternal(
      submissionParam: SubmissionParam,
      submissionId: steps.PendingSubmissionId,
      messageId: MessageId,
      batch: Batch[DefaultOpenEnvelope],
      maxSequencingTime: CantonTimestamp,
      embedSubmissionError: SequencerRequestError => steps.SubmissionSendError,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    steps.SubmissionSendError,
    (SendResult, steps.SubmissionResultArgs),
  ] = {
    def removePendingSubmission(): Unit = {
      steps
        .removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId)
        .discard[Option[steps.PendingSubmissionData]]
    }

    for {
      // The pending submission must be registered before the request is sent, to avoid races
      resultArgs <- steps
        .updatePendingSubmissions(
          steps.pendingSubmissions(ephemeral),
          submissionParam,
          submissionId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      _ = logger.info(
        s"Phase 1 completed: Submitting ${batch.envelopes.length} envelopes for ${steps.requestKind} request, ${steps
            .submissionDescription(submissionParam)}"
      )

      // use the send callback and a promise to capture the eventual sequenced event read by the submitter
      sendResultP = mkPromise[SendResult](
        "sequenced-event-send-result",
        futureSupervisor,
      )

      _ <- sequencerClient
        .sendAsync(
          batch,
          callback = res => sendResultP.trySuccess(res).discard,
          maxSequencingTime = maxSequencingTime,
          messageId = messageId,
          amplify = true,
        )
        .leftMap { err =>
          removePendingSubmission()
          embedSubmissionError(SequencerRequestError(err))
        }

      // If we're shutting down, the sendResult below won't complete successfully (because it's wrapped in a `FutureUnlessShutdown`)
      // We still want to clean up pending submissions in that case though so make sure we do that by adding a callback on
      // the promise here
      _ = sendResultP.future.onComplete {
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          logger.debug(s"Submission $submissionId aborted due to shutdown")
          removePendingSubmission()
        case _ =>
      }

      sendResult <- EitherT.right(FutureUnlessShutdown(sendResultP.future))
    } yield {
      SendResult.log("Submission", logger)(UnlessShutdown.Outcome(sendResult))

      sendResult match {
        case SendResult.Success(deliver) =>
          schedulePendingSubmissionRemoval(deliver.timestamp, submissionId)
        case SendResult.Error(_) | SendResult.Timeout(_) =>
          removePendingSubmission()
      }

      sendResult -> resultArgs
    }
  }

  /** Schedules removal of the pending submission once the request tracker has advanced to the decision time.
    * This happens if the request times out (w.r.t. the submission timestamp) or the sequencer never delivers a request.
    */
  private def schedulePendingSubmissionRemoval(
      submissionTimestamp: CantonTimestamp,
      submissionId: steps.PendingSubmissionId,
  )(implicit traceContext: TraceContext): Unit = {

    val removeF = for {
      domainParameters <- crypto.ips
        .awaitSnapshot(submissionTimestamp)
        .flatMap(_.findDynamicDomainParameters())
        .flatMap(_.toFuture(new RuntimeException(_)))

      decisionTime <- domainParameters.decisionTimeForF(submissionTimestamp)
      _ = ephemeral.timeTracker.requestTick(decisionTime)
      _ <- ephemeral.requestTracker.awaitTimestamp(decisionTime).getOrElse(Future.unit).map { _ =>
        steps.removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId).foreach {
          submissionData =>
            logger.debug(s"Removing sent submission $submissionId without a result.")
            steps.postProcessResult(
              Verdict.ParticipantReject(
                NonEmpty(
                  List,
                  Set.empty[LfPartyId] ->
                    LocalRejectError.TimeRejects.LocalTimeout
                      .Reject()
                      .toLocalReject(protocolVersion),
                ),
                protocolVersion,
              ),
              submissionData,
            )
        }
      }
    } yield ()

    FutureUtil.doNotAwait(removeF, s"Failed to remove the pending submission $submissionId")
  }

  private def handlerResultForRequest(
      ts: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.RequestError,
        EitherT[FutureUnlessShutdown, steps.RequestError, Unit],
      ],
  )(implicit traceContext: TraceContext): HandlerResult = {
    // We discard the lefts because they are logged by `logRequestWarnings`
    logRequestWarnings(ts, result)
      .map(innerAsync => AsyncResult(innerAsync.getOrElse(())))
      .getOrElse(AsyncResult.immediate)
  }

  private[this] def logRequestWarnings(
      resultTimestamp: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.RequestError,
        EitherT[FutureUnlessShutdown, steps.RequestError, Unit],
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, EitherT[
    FutureUnlessShutdown,
    steps.RequestError,
    Unit,
  ]] = {

    def doLog[T](
        result: EitherT[FutureUnlessShutdown, steps.RequestError, T]
    ): EitherT[FutureUnlessShutdown, steps.RequestError, T] =
      EitherTUtil.logOnErrorU(
        result,
        s"${steps.requestKind} ${RequestId(resultTimestamp)}: Failed to process request",
      )

    doLog(result.map(doLog))
  }

  override def processRequest(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      batch: steps.RequestBatch,
  )(implicit traceContext: TraceContext): HandlerResult = {
    val RequestAndRootHashMessage(viewMessages, rootHashMessage, mediatorId, _isReceipt) = batch
    val requestId = RequestId(ts)

    if (precedesCleanReplay(requestId)) {
      // The `MessageDispatcher` should not call this method for requests before the clean replay starting point
      HandlerResult.synchronous(
        ErrorUtil.internalErrorAsyncShutdown(
          new IllegalArgumentException(
            s"Request with timestamp $ts precedes the clean replay starting point"
          )
        )
      )
    } else {
      logger.info(
        show"Phase 3: Validating ${steps.requestKind.unquoted} request=${requestId.unwrap} with ${batch.requestEnvelopes.length} envelope(s)"
      )

      val rootHash = batch.rootHashMessage.rootHash
      val freshOwnTimelyTxF = ephemeral.submissionTracker.register(rootHash, requestId)

      val processedET = performUnlessClosingEitherU(
        s"ProtocolProcess.processRequest(rc=$rc, sc=$sc, traceId=${traceContext.traceId})"
      ) {
        // registering the request has to be done synchronously
        EitherT
          .rightT[Future, ProtocolProcessor.this.steps.RequestError](
            ephemeral.phase37Synchronizer
              .registerRequest(steps.requestType)(RequestId(ts))
          )
          .map { requestDataHandle =>
            // If the result is not a success, we still need to complete the request data in some way
            processRequestInternal(ts, rc, sc, batch, requestDataHandle, freshOwnTimelyTxF)
              .thereafter {
                case Failure(exception) => requestDataHandle.failed(exception)
                case Success(UnlessShutdown.Outcome(Left(_))) => requestDataHandle.complete(None)
                case Success(UnlessShutdown.AbortedDueToShutdown) => requestDataHandle.shutdown()
                case _ =>
              }
          }
      }
      handlerResultForRequest(ts, processedET)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  @VisibleForTesting
  private[protocol] def processRequestInternal(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      batch: steps.RequestBatch,
      requestDataHandle: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      freshOwnTimelyTxF: FutureUnlessShutdown[Boolean],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {
    val RequestAndRootHashMessage(viewMessages, rootHashMessage, mediator, isReceipt) = batch
    val requestId = RequestId(ts)
    val rootHash = rootHashMessage.rootHash

    def checkRootHash(
        decryptedViews: Seq[(WithRecipients[steps.DecryptedView], Option[Signature])]
    ): (Seq[MalformedPayload], Seq[(WithRecipients[steps.DecryptedView], Option[Signature])]) = {

      val correctRootHash = rootHashMessage.rootHash
      val (viewsWithCorrectRootHash, viewsWithWrongRootHash) =
        decryptedViews.partition { case (view, _) => view.unwrap.rootHash == correctRootHash }
      val malformedPayloads: Seq[MalformedPayload] =
        viewsWithWrongRootHash.map { case (viewTree, _) =>
          ProtocolProcessor.WrongRootHash(viewTree.unwrap, correctRootHash)
        }

      (malformedPayloads, viewsWithCorrectRootHash)
    }

    def observeSequencedRootHash(amSubmitter: Boolean): Future[Unit] =
      if (amSubmitter && !isReceipt) {
        // We are the submitting participant and yet the request does not have a message ID.
        // This looks like a preplay attack, and we mark the request as sequenced in the in-flight
        // submission tracker to avoid the situation that our original submission never gets sequenced
        // and gets picked up by a timely rejection, which would emit a duplicate command completion.
        val sequenced = SequencedSubmission(sc, ts)
        inFlightSubmissionTracker.observeSequencedRootHash(
          rootHash,
          sequenced,
        )
      } else Future.unit

    performUnlessClosingEitherUSF(
      s"$functionFullName(rc=$rc, sc=$sc, traceId=${traceContext.traceId})"
    ) {
      val preliminaryChecksET = for {
        snapshot <- EitherT.right(
          crypto.awaitSnapshotUSSupervised(s"await crypto snapshot $ts")(ts)
        )
        domainParameters <- EitherT(
          snapshot.ipsSnapshot
            .findDynamicDomainParameters()
            .map(
              _.leftMap(_ =>
                steps.embedRequestError(
                  UnableToGetDynamicDomainParameters(
                    snapshot.domainId,
                    snapshot.ipsSnapshot.timestamp,
                  )
                )
              )
            )
        ).mapK(FutureUnlessShutdown.outcomeK)

        decryptedViews <- steps
          .decryptViews(viewMessages, snapshot, ephemeral.sessionKeyStore)
      } yield (snapshot, decryptedViews, domainParameters)

      for {
        preliminaryChecks <- preliminaryChecksET.leftMap { err =>
          ephemeral.submissionTracker.cancelRegistration(rootHash, requestId)
          err
        }
        (snapshot, uncheckedDecryptedViews, domainParameters) = preliminaryChecks

        steps.DecryptedViews(decryptedViewsWithSignatures, rawDecryptionErrors) =
          uncheckedDecryptedViews
        _ = rawDecryptionErrors.foreach { decryptionError =>
          logger.warn(s"Request $rc: Decryption error: $decryptionError")
        }
        decryptionErrors = rawDecryptionErrors.map(ViewMessageError(_))

        (incorrectRootHashes, viewsWithCorrectRootHash) = checkRootHash(
          decryptedViewsWithSignatures
        )
        _ = incorrectRootHashes.foreach { incorrectRootHash =>
          logger.warn(s"Request $rc: Found malformed payload: $incorrectRootHash")
        }

        (submitterMetadataO, submissionDataForTrackerO) = steps.getSubmitterInformation(
          viewsWithCorrectRootHash.map { case (view, _) => view.unwrap }
        )

        checkRecipientsResult <- EitherT.right(
          FutureUnlessShutdown.outcomeF(
            recipientsValidator.retainInputsWithValidRecipients(
              requestId,
              viewsWithCorrectRootHash,
              snapshot.ipsSnapshot,
            )
          )
        )
        (incorrectRecipients, viewsWithCorrectRootHashAndRecipients) = checkRecipientsResult

        (fullViewsWithCorrectRootHashAndRecipients, incorrectDecryptedViews) =
          steps.computeFullViews(viewsWithCorrectRootHashAndRecipients)

        malformedPayloads =
          decryptionErrors ++ incorrectRootHashes ++ incorrectRecipients ++ incorrectDecryptedViews

        _ <- NonEmpty.from(fullViewsWithCorrectRootHashAndRecipients) match {
          case None =>
            /*
              If fullViewsWithCorrectRootHashAndRecipients is empty, it does not necessarily mean that we have a
              malicious submitter (e.g., if there is concurrent topology change). Hence, if we have a submission data,
              then we will aim to generate a command completion.
             */
            submissionDataForTrackerO match {
              // TODO(i17075): study scenarios exploitable by honest-but-curious sequencers
              case Some(submissionDataForTracker) =>
                ephemeral.submissionTracker.provideSubmissionData(
                  rootHash,
                  requestId,
                  submissionDataForTracker,
                )

                val error =
                  TransactionProcessor.SubmissionErrors.NoViewWithValidRecipients.Error(ts)

                for {
                  _ <- EitherT
                    .right(
                      observeSequencedRootHash(
                        submissionDataForTracker.submittingParticipant == participantId
                      )
                    )
                    .mapK(FutureUnlessShutdown.outcomeK)
                  _ <- stopRequestProcessing(
                    ts,
                    rc,
                    sc,
                    requestDataHandle,
                    submitterMetadataO,
                    rootHash,
                    freshOwnTimelyTxF,
                    error,
                  )
                } yield ()

              case None =>
                // We were not able to find submitter metadata within the decrypted views with correct root hash.
                // Either there is no such view, or none of them are root views.
                // In any case, we don't need to generate a command completion.
                ephemeral.submissionTracker.cancelRegistration(rootHash, requestId)
                trackAndSendResponsesMalformed(
                  rc,
                  sc,
                  ts,
                  rootHash,
                  requestDataHandle,
                  mediator,
                  snapshot,
                  malformedPayloads,
                )
            }

          case Some(goodViewsWithSignatures) =>
            // All views with the same correct root hash declare the same mediator, so it's enough to look at the head
            val (firstView, _) = goodViewsWithSignatures.head1

            val observeF = submissionDataForTrackerO match {
              case Some(submissionDataForTracker) =>
                ephemeral.submissionTracker.provideSubmissionData(
                  rootHash,
                  requestId,
                  submissionDataForTracker,
                )

                observeSequencedRootHash(
                  submissionDataForTracker.submittingParticipant == participantId
                )
              case None =>
                // There are no root views
                ephemeral.submissionTracker.cancelRegistration(
                  rootHash,
                  requestId,
                )

                Future.unit
            }

            val declaredMediator = firstView.unwrap.mediator
            // Lazy so as to prevent this running concurrently with `observeF`
            lazy val processF = if (declaredMediator == mediator) {

              for {
                isFreshOwnTimelyRequest <- EitherT.right(freshOwnTimelyTxF)

                parsedRequest <- EitherT
                  .right(
                    steps.computeParsedRequest(
                      rc,
                      ts,
                      sc,
                      goodViewsWithSignatures,
                      submitterMetadataO,
                      isFreshOwnTimelyRequest,
                      malformedPayloads,
                      mediator,
                      snapshot,
                      domainParameters,
                    )
                  )
                  .mapK(FutureUnlessShutdown.outcomeK)

                _ <- processRequestWithGoodViews(
                  parsedRequest,
                  requestDataHandle,
                )
              } yield ()
            } else {
              // When the mediator `mediatorId` receives the root hash message,
              // it will either lack the full informee tree or find the wrong mediator ID in it.
              // The submitting participant is malicious (unless the sequencer is), so it is not this participant
              // and therefore we don't have to output a completion event
              logger.error(
                s"Mediator $declaredMediator declared in views is not the recipient $mediator of the root hash message"
              )
              EitherT
                .right[steps.RequestError](
                  prepareForMediatorResultOfBadRequest(rc, sc, ts)
                )
                .thereafter(_ => requestDataHandle.complete(None))
            }

            for {
              _ <- EitherT.right(observeF).mapK(FutureUnlessShutdown.outcomeK)
              _ <- processF
            } yield ()
        }
      } yield ()
    }
  }

  private def stopRequestProcessing(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      requestDataHandle: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      submitterMetadataO: Option[steps.ViewSubmitterMetadata],
      rootHash: RootHash,
      freshOwnTimelyTxF: FutureUnlessShutdown[Boolean],
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {
    for {
      freshOwnTimelyTx <- EitherT.right(freshOwnTimelyTxF)

      (eventO, submissionIdO) = submitterMetadataO
        .map { submitterMetadata =>
          steps.eventAndSubmissionIdForRejectedCommand(
            ts,
            rc,
            sc,
            submitterMetadata,
            rootHash,
            freshOwnTimelyTx,
            error,
          )
        }
        .getOrElse((None, None))

      _ <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          unlessCleanReplay(rc)(
            ephemeral.recordOrderPublisher.schedulePublication(sc, rc, ts, eventO)
          )
        )
      )
      pendingSubmissionDataO = submissionIdO.flatMap(submissionId =>
        // This removal does not interleave with `schedulePendingSubmissionRemoval`
        // as the sequencer respects the max sequencing time of the request.
        // TODO(M99) Gracefully handle the case that the sequencer does not respect the max sequencing time.
        steps.removePendingSubmission(
          steps.pendingSubmissions(ephemeral),
          submissionId,
        )
      )
      _ = pendingSubmissionDataO.foreach(
        steps.postProcessSubmissionRejectedCommand(error, _)
      )
      _ <- EitherT.right[steps.RequestError] {
        requestDataHandle.complete(None)
        invalidRequest(rc, sc, ts)
      }
    } yield ()
  }

  private def processRequestWithGoodViews(
      parsedRequest: steps.ParsedRequestType,
      requestDataHandle: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ProtocolProcessor.this.steps.RequestError, Unit] = {
    val rc = parsedRequest.rc
    val sc = parsedRequest.sc
    val ts = parsedRequest.requestTimestamp
    val mediator = parsedRequest.mediator

    // Check whether the declared mediator is still an active mediator.
    for {
      mediatorIsActive <- EitherT
        .right(parsedRequest.snapshot.ipsSnapshot.isMediatorActive(mediator))
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (mediatorIsActive)
          for {
            activenessSet <- EitherT.fromEither[FutureUnlessShutdown](
              steps
                .computeActivenessSet(parsedRequest)
            )
            _ <- trackAndSendResponsesWellformed(
              parsedRequest,
              activenessSet,
              requestDataHandle,
            )
          } yield ()
        else {
          SyncServiceAlarm
            .Warn(
              s"Request $rc: Chosen mediator $mediator is inactive at $ts. Skipping this request."
            )
            .report()

          // The chosen mediator may have become inactive between submission and sequencing.
          // All honest participants and the mediator will ignore the request,
          // but the submitting participant still must produce a completion event.
          val error =
            TransactionProcessor.SubmissionErrors.InactiveMediatorError
              .Error(mediator, ts)

          stopRequestProcessing(
            ts,
            rc,
            sc,
            requestDataHandle,
            parsedRequest.submitterMetadataO,
            parsedRequest.rootHash,
            FutureUnlessShutdown.pure(parsedRequest.isFreshOwnTimelyRequest),
            error,
          )
        }
    } yield ()
  }

  /** Updates trackers and sends confirmation responses in the case that at least one view is wellformed. */
  private def trackAndSendResponsesWellformed(
      parsedRequest: steps.ParsedRequestType,
      activenessSet: ActivenessSet,
      requestDataHandle: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {
    val requestId = parsedRequest.requestId
    val rc = parsedRequest.rc
    val sc = parsedRequest.sc
    val ts = parsedRequest.requestTimestamp
    val mediator = parsedRequest.mediator
    val decisionTime = parsedRequest.decisionTime

    val engineController = EngineController(
      participantId,
      requestId,
      loggerFactory,
      testingConfig.reinterpretationTestHookFor,
    )

    for {
      requestFuturesF <- EitherT
        .fromEither[FutureUnlessShutdown](
          ephemeral.requestTracker
            .addRequest(rc, sc, ts, ts, decisionTime, activenessSet)
        )
        .leftMap(err => steps.embedRequestError(RequestTrackerError(err)))

      _ <- steps
        .authenticateInputContracts(parsedRequest)
        .mapK(FutureUnlessShutdown.outcomeK)

      pendingDataAndResponsesAndTimeoutEvent <-
        if (isCleanReplay(rc)) {
          val pendingData =
            CleanReplayData(
              rc,
              sc,
              mediator,
              locallyRejectedF = FutureUnlessShutdown.pure(false),
              abortEngine = _ => (), // No need to abort
              engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
            )
          val responses = EitherT.pure[FutureUnlessShutdown, steps.RequestError](
            Seq.empty[(ConfirmationResponse, Recipients)]
          )
          val timeoutEvent = Either.right(Option.empty[TimestampedEvent])
          EitherT.pure[FutureUnlessShutdown, steps.RequestError](
            (pendingData, responses, () => timeoutEvent)
          )
        } else {
          for {
            _ <- EitherT.right(
              FutureUnlessShutdown.outcomeF(ephemeral.requestJournal.insert(rc, ts))
            )

            pendingDataAndResponses <- steps.constructPendingDataAndResponse(
              parsedRequest,
              ephemeral.transferCache,
              requestFuturesF.flatMap(_.activenessResult),
              engineController,
            )

            steps.StorePendingDataAndSendResponseAndCreateTimeout(
              pendingData,
              responsesF,
              rejectionArgs,
            ) = pendingDataAndResponses
            PendingRequestData(
              pendingRequestCounter,
              pendingSequencerCounter,
              _,
              _locallyRejected,
            ) = pendingData
            _ = if (
              pendingRequestCounter != rc
              || pendingSequencerCounter != sc
            )
              throw new RuntimeException("Pending result data inconsistent with request")

          } yield (
            Wrapped(pendingData),
            responsesF,
            () => steps.createRejectionEvent(rejectionArgs),
          )
        }

      (
        pendingData,
        responsesToET,
        timeoutEvent,
      ) =
        pendingDataAndResponsesAndTimeoutEvent

      // Make sure activeness result finished
      requestFutures <- EitherT.right[steps.RequestError](requestFuturesF)
      _activenessResult <- EitherT.right[steps.RequestError](requestFutures.activenessResult)

      _ = requestDataHandle.complete(Some(pendingData))
      // Request to observe a timestamp >= the decision time, so that the timeout can be triggered
      _ = ephemeral.timeTracker.requestTick(decisionTime)
      timeoutET = EitherT
        .right(requestFutures.timeoutResult)
        .flatMap(
          handleTimeout(
            requestId,
            rc,
            sc,
            decisionTime,
            timeoutEvent(),
          )
        )
      _ = EitherTUtil.doNotAwaitUS(timeoutET, "Handling timeout failed")

      responsesTo <- responsesToET
      signedResponsesTo <- EitherT.right(responsesTo.parTraverse { case (response, recipients) =>
        signResponse(parsedRequest.snapshot, response).map(_ -> recipients)
      })
      engineAbortStatus <- EitherT.right(pendingData.engineAbortStatusF)
      _ <-
        if (engineAbortStatus.isAborted) {
          // There is no point in sending a response if we have aborted
          logger.info(
            s"Phase 4: Finished validation for request=${requestId.unwrap} with abort."
          )
          EitherTUtil.unitUS[steps.RequestError]
        } else if (signedResponsesTo.nonEmpty) {
          val messageId = sequencerClient.generateMessageId
          logger.info(
            s"Phase 4: Sending for request=${requestId.unwrap} with msgId=${messageId} ${val (approved, rejected) =
                signedResponsesTo
                  .foldLeft((0, 0)) { case ((app, rej), (response, _)) =>
                    response.message.localVerdict match {
                      case LocalApprove() => (app + 1, rej)
                      case _: LocalReject => (app, rej + 1)
                    }
                  }
              s"approved=${approved}, rejected=${rejected}" }"
          )
          EitherT.liftF[FutureUnlessShutdown, steps.RequestError, Unit](
            sendResponses(requestId, signedResponsesTo, Some(messageId))
          )
        } else {
          logger.info(
            s"Phase 4: Finished validation for request=${requestId.unwrap} with nothing to approve."
          )
          EitherTUtil.unitUS[steps.RequestError]
        }

    } yield ()

  }

  /** Updates trackers and sends confirmation responses in the case that all views are malformed. */
  private def trackAndSendResponsesMalformed(
      rc: RequestCounter,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      rootHash: RootHash,
      requestDataHandle: Phase37Synchronizer.PendingRequestDataHandle[
        steps.requestType.PendingRequestData
      ],
      mediatorGroup: MediatorGroupRecipient,
      snapshot: DomainSnapshotSyncCryptoApi,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.RequestError, Unit] = {

    val requestId = RequestId(ts)

    if (isCleanReplay(rc)) {
      ephemeral.requestTracker.tick(sc, ts)
      EitherTUtil.unitUS
    } else {
      for {
        _ <- EitherT
          .right(ephemeral.requestJournal.insert(rc, ts))
          .mapK(FutureUnlessShutdown.outcomeK)

        _ = ephemeral.requestTracker.tick(sc, ts)

        responses = steps.constructResponsesForMalformedPayloads(
          requestId,
          rootHash,
          malformedPayloads,
        )
        recipients = Recipients.cc(mediatorGroup)
        messages <- EitherT
          .right(responses.parTraverse { response =>
            signResponse(snapshot, response).map(_ -> recipients)
          })

        _ <- EitherT.liftF(sendResponses(requestId, messages))

        _ = requestDataHandle.complete(None)

        _ <- EitherT.right[steps.RequestError](
          FutureUnlessShutdown.outcomeF(terminateRequest(rc, sc, ts, ts))
        )
      } yield ()
    }
  }

  private def handlerResultForConfirmationResult(
      ts: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.ResultError,
        EitherT[FutureUnlessShutdown, steps.ResultError, Unit],
      ],
  )(implicit traceContext: TraceContext): HandlerResult = {
    // We discard the lefts because they are logged by `logResultWarnings`
    logResultWarnings(ts, result)
      .map(innerAsync => AsyncResult(innerAsync.getOrElse(())))
      .getOrElse(AsyncResult.immediate)
  }

  override def processResult(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]
  )(implicit traceContext: TraceContext): HandlerResult = {
    val content = event.event.content
    val ts = content.timestamp
    val sc = content.counter

    val processedET = performUnlessClosingEitherU(
      s"ProtocolProcess.processResult(sc=$sc, traceId=${traceContext.traceId}"
    ) {
      val resultEnvelopes =
        content.batch.envelopes
          .mapFilter(ProtocolMessage.select[SignedProtocolMessage[ConfirmationResultMessage]])
      ErrorUtil.requireArgument(
        resultEnvelopes.sizeCompare(1) == 0,
        steps.requestKind + " result contains multiple such messages",
      )

      val result = resultEnvelopes(0).protocolMessage
      val requestId = result.message.requestId

      logger.debug(
        show"Got result for ${steps.requestKind.unquoted} request at $requestId: $resultEnvelopes"
      )

      processResultInternal1(event, result, requestId, ts, sc)
    }

    handlerResultForConfirmationResult(ts, processedET)
  }

  @VisibleForTesting
  private[protocol] def processResultInternal1(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      result: SignedProtocolMessage[ConfirmationResultMessage],
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.ResultError, EitherT[FutureUnlessShutdown, steps.ResultError, Unit]] = {
    ephemeral.recordOrderPublisher.tick(sc, resultTs)

    val snapshotTs = requestId.unwrap

    for {
      snapshot <- EitherT.right(
        crypto.ips.awaitSnapshotSupervised(s"await crypto snapshot $snapshotTs")(snapshotTs)
      )

      domainParameters <- EitherT(
        snapshot
          .findDynamicDomainParameters()
          .map(
            _.leftMap(_ =>
              steps.embedResultError(
                UnableToGetDynamicDomainParameters(
                  domainId,
                  requestId.unwrap,
                )
              )
            )
          )
      )

      decisionTime = domainParameters
        .decisionTimeFor(requestId.unwrap)
        .valueOr(err =>
          // This should not happen as domainParameters come from snapshot at requestId
          throw new IllegalStateException(err)
        )

      participantDeadline = domainParameters
        .participantResponseDeadlineFor(requestId.unwrap)
        .valueOr(err =>
          // This should not happen as domainParameters come from snapshot at requestId
          throw new IllegalStateException(err)
        )

      _ <- condUnitET[Future](
        resultTs <= decisionTime, {
          ephemeral.requestTracker.tick(sc, resultTs)
          steps.embedResultError(DecisionTimeElapsed(requestId, resultTs))
          /* We must not evict the request from `pendingRequestData` or `pendingSubmissionMap`
           * because this will have been taken care of by `handleTimeout`
           * when the request tracker progresses to the decision time.
           */
        },
      )
      _ <- EitherT.cond[Future](
        resultTs > participantDeadline || !result.message.verdict.isTimeoutDeterminedByMediator,
        (), {
          SyncServiceAlarm
            .Warn(
              s"Received mediator timeout message at $resultTs before response deadline for request $requestId."
            )
            .report()

          ephemeral.requestTracker.tick(sc, resultTs)
          steps.embedResultError(TimeoutResultTooEarly(requestId))
        },
      )
      asyncResult <-
        if (!precedesCleanReplay(requestId))
          processResultInternal2(
            event,
            result,
            requestId,
            resultTs,
            sc,
            domainParameters,
          )
        else
          EitherT.pure[Future, steps.ResultError](
            EitherT.pure[FutureUnlessShutdown, steps.ResultError](())
          )
    } yield asyncResult
  }

  /** This processing step corresponds to the end of the synchronous part of the processing
    * of confirmation result.
    * The inner `EitherT` corresponds to the subsequent async stage.
    */
  private[this] def processResultInternal2(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      result: SignedProtocolMessage[ConfirmationResultMessage],
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.ResultError, EitherT[FutureUnlessShutdown, steps.ResultError, Unit]] = {
    val unsignedResult = result.message

    def filterInvalidSignature(
        pendingRequestData: PendingRequestData
    ): Future[Boolean] =
      for {
        snapshot <- crypto.awaitSnapshot(requestId.unwrap)
        res <- result.verifyMediatorSignatures(snapshot, pendingRequestData.mediator.group).value
      } yield {
        res match {
          case Left(err) =>
            SyncServiceAlarm
              .Warn(
                s"Received a confirmation result at $resultTs for $requestId " +
                  s"with an invalid signature for ${pendingRequestData.mediator}. Discarding message... Details: $err"
              )
              .report()
            false
          case Right(()) =>
            true
        }
      }

    def filterInvalidRootHash(
        pendingRequestDataOrReplayData: PendingRequestData
    ): Future[Boolean] = Future.successful {
      pendingRequestDataOrReplayData.rootHashO.forall { txRootHash =>
        val resultRootHash = unsignedResult.rootHash
        val rootHashMatches = resultRootHash == txRootHash

        if (!rootHashMatches) {
          val cause =
            s"Received a confirmation result message at $resultTs from ${pendingRequestDataOrReplayData.mediator} " +
              s"for $requestId with an invalid root hash $resultRootHash instead of $txRootHash. Discarding message..."
          SyncServiceAlarm.Warn(cause).report()
        }

        rootHashMatches
      }
    }

    val combinedFilter =
      (prd: PendingRequestData) =>
        MonadUtil
          .foldLeftM(true, Seq(filterInvalidSignature _, filterInvalidRootHash _))((acc, x) =>
            if (acc) x(prd) else Future.successful(acc)
          )

    // Wait until we have processed the corresponding request
    //
    // This may deadlock if we haven't received the `requestId` as a request.
    // For example, if there never was a request with the given timestamp,
    // then the phase 3-7 synchronizer waits until the all requests until `requestId`'s timestamp
    // and the next request have reached `Confirmed`.
    // However, if there was no request between `requestId` and `ts`,
    // then the next request will not reach `Confirmed`
    // because the request tracker will not progress beyond `ts` as the `tick` for `ts` comes only after this point.
    // Accordingly, time proofs will not trigger a timeout either.
    //
    // We don't know whether any protocol processor has ever seen the request with `requestId`;
    // it might be that the message dispatcher already decided that the request is malformed and should not be processed.
    // In this case, the message dispatcher has assigned a request counter to the request if it expects to get a confirmation result
    // and the BadRootHashMessagesRequestProcessor moved the request counter to `Confirmed`.
    // So the deadlock should happen only if the mediator or sequencer are dishonest.
    //
    // TODO(M99) This argument relies on the mediator sending a MalformedMediatorConfirmationRequest only to participants
    //  that have also received a message with the request.
    //  A dishonest sequencer or mediator could break this assumption.

    // Some more synchronization is done in the Phase37Synchronizer.
    val res = performUnlessClosingEitherUSF(
      s"$functionFullName(sc=$sc, traceId=${traceContext.traceId})"
    )(
      EitherT(
        ephemeral.phase37Synchronizer
          .awaitConfirmed(steps.requestType)(requestId, combinedFilter)
          .map {
            case RequestOutcome.Success(pendingRequestData) =>
              Right(pendingRequestData)
            case RequestOutcome.AlreadyServedOrTimeout =>
              ephemeral.requestTracker.tick(sc, resultTs)
              Left(steps.embedResultError(UnknownPendingRequest(requestId)))
            case RequestOutcome.Invalid =>
              ephemeral.requestTracker.tick(sc, resultTs)
              Left(steps.embedResultError(InvalidPendingRequest(requestId)))
          }
      ).flatMap { pendingRequestDataOrReplayData =>
        processResultInternal3(
          event,
          unsignedResult.verdict,
          requestId,
          resultTs,
          sc,
          domainParameters,
          pendingRequestDataOrReplayData,
        )
      }
    )

    // This is now lifted to the asynchronous part of the processing.
    EitherT.pure(res)
  }

  // The processing in this method is done in the asynchronous part of the processing
  private[this] def processResultInternal3(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      requestId: RequestId,
      resultTs: CantonTimestamp,
      sc: SequencerCounter,
      domainParameters: DynamicDomainParametersWithValidity,
      pendingRequestDataOrReplayData: ReplayDataOr[
        steps.requestType.PendingRequestData
      ],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, steps.ResultError, Unit] = {
    // If we have received a negative verdict, we will not need the result of the engine computation, and
    // we can therefore abort. If the computation has already completed, this will have no effect.
    if (!verdict.isApprove)
      pendingRequestDataOrReplayData.abortEngine(s"received negative mediator verdict $verdict")

    val PendingRequestData(requestCounter, requestSequencerCounter, _, locallyRejectedF) =
      pendingRequestDataOrReplayData
    val cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)
    val pendingSubmissionDataO = removePendingSubmissionForRequest(pendingRequestDataOrReplayData)

    for {
      // TODO(i15395): handle this more gracefully
      locallyRejected <- EitherT.right(locallyRejectedF)
      _ = checkContradictoryMediatorApprove(locallyRejected, verdict)

      commitAndEvent <- pendingRequestDataOrReplayData match {
        case Wrapped(pendingRequestData) =>
          for {
            commitSetAndContractsAndEvent <- steps
              .getCommitSetAndContractsToBeStoredAndEvent(
                event,
                verdict,
                pendingRequestData,
                steps.pendingSubmissions(ephemeral),
                crypto.pureCrypto,
              )
          } yield {
            val steps.CommitAndStoreContractsAndPublishEvent(
              commitSetOF,
              contractsToBeStored,
              eventO,
            ) = commitSetAndContractsAndEvent

            val isApproval = verdict.isApprove

            if (!isApproval && commitSetOF.isDefined)
              throw new RuntimeException("Negative verdicts entail an empty commit set")

            (commitSetOF, contractsToBeStored, eventO)
          }
        case _: CleanReplayData =>
          val commitSetOF = Option.when(verdict.isApprove)(Future.successful(CommitSet.empty))
          val eventO = None

          EitherT.pure[FutureUnlessShutdown, steps.ResultError](
            (commitSetOF, Seq.empty, eventO)
          )
      }
      (commitSetOF, contractsToBeStored, eventO) = commitAndEvent

      commitTime = resultTs
      commitSetF <- signalResultToRequestTracker(
        requestCounter,
        sc,
        requestId,
        resultTs,
        commitTime,
        commitSetOF,
        domainParameters,
      ).leftMap(err => steps.embedResultError(RequestTrackerError(err)))
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          ephemeral.contractStore.storeCreatedContracts(
            contractsToBeStored.map((_, requestCounter))
          )
        )
      )

      _ <- ifThenET(!cleanReplay) {
        for {
          _unit <- {
            logger.debug(
              show"Finalizing ${steps.requestKind.unquoted} request=${requestId.unwrap} with event $eventO."
            )

            // Schedule publication of the event with the associated causality update.
            // Note that both fields are optional.
            // Some events (such as rejection events) are not associated with causality updates.
            // Additionally, we may process a causality update without an associated event (this happens on transfer-in)
            EitherT.right[steps.ResultError](
              FutureUnlessShutdown.outcomeF(
                ephemeral.recordOrderPublisher
                  .schedulePublication(
                    requestSequencerCounter,
                    requestCounter,
                    requestId.unwrap,
                    eventO,
                  )
              )
            )
          }

          commitSet <- EitherT.right[steps.ResultError](commitSetF)
          _ = ephemeral.recordOrderPublisher.scheduleAcsChangePublication(
            requestSequencerCounter,
            requestId.unwrap,
            requestCounter,
            commitSet,
          )
          requestTimestamp = requestId.unwrap
          _unit <- EitherT.right[steps.ResultError](
            FutureUnlessShutdown.outcomeF(
              terminateRequest(
                requestCounter,
                requestSequencerCounter,
                requestTimestamp,
                commitTime,
              )
            )
          )
        } yield pendingSubmissionDataO.foreach(steps.postProcessResult(verdict, _))
      }
    } yield ()
  }

  private def checkContradictoryMediatorApprove(
      locallyRejected: Boolean,
      verdict: Verdict,
  )(implicit traceContext: TraceContext): Unit = {
    if (
      isApprovalContradictionCheckEnabled(
        loggerFactory.name
      ) && verdict.isApprove && locallyRejected
    ) {
      ErrorUtil.invalidState(s"Mediator approved a request that we have locally rejected")
    }
  }

  private[this] def logResultWarnings(
      resultTimestamp: CantonTimestamp,
      result: EitherT[
        FutureUnlessShutdown,
        steps.ResultError,
        EitherT[FutureUnlessShutdown, steps.ResultError, Unit],
      ],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.ResultError, EitherT[
    FutureUnlessShutdown,
    steps.ResultError,
    Unit,
  ]] = {

    def logResultWarnings[T](
        result: EitherT[FutureUnlessShutdown, steps.ResultError, T],
        default: T,
    ): EitherT[FutureUnlessShutdown, steps.ResultError, T] = {
      val warningsLogged = EitherTUtil.leftSubflatMap(result) { processorError =>
        processorError.underlyingProcessorError() match {
          case Some(DecisionTimeElapsed(requestId, _)) =>
            logger.warn(
              show"${steps.requestKind.unquoted} request at $requestId: Result arrived after the decision time (arrived at $resultTimestamp)"
            )
            Right(default)
          case Some(UnknownPendingRequest(requestId)) =>
            // the mediator can send duplicate transaction results during crash recovery and fail over, triggering this error
            logger.info(
              show"${steps.requestKind.unquoted} request at $requestId: Received event at $resultTimestamp for request that is not pending"
            )
            Right(default)
          case Some(InvalidPendingRequest(requestId)) =>
            logger.info(
              show"${steps.requestKind.unquoted} request at $requestId: Received event at $resultTimestamp for request that is invalid"
            )
            Right(default)
          case err => Left(processorError)
        }
      }

      EitherTUtil.logOnErrorU(warningsLogged, s"${steps.requestKind}: Failed to process result")
    }

    logResultWarnings(
      result.map(logResultWarnings(_, ())),
      EitherT.pure[FutureUnlessShutdown, steps.ResultError](()),
    )
  }

  private[this] def removePendingSubmissionForRequest(
      pendingRequestDataOrReplayData: ReplayDataOr[
        steps.requestType.PendingRequestData
      ]
  ): Option[steps.PendingSubmissionData] =
    for {
      pendingRequestData <- pendingRequestDataOrReplayData.toOption
      submissionId = steps.submissionIdOfPendingRequest(pendingRequestData)
      submissionData <- steps.removePendingSubmission(
        steps.pendingSubmissions(ephemeral),
        submissionId,
      )
    } yield submissionData

  private def signalResultToRequestTracker(
      rc: RequestCounter,
      sc: SequencerCounter,
      requestId: RequestId,
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
      commitSetOF: Option[Future[CommitSet]],
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, RequestTracker.RequestTrackerError, FutureUnlessShutdown[CommitSet]] = {

    def withRc(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

    val requestTimestamp = requestId.unwrap

    ErrorUtil.requireArgument(
      resultTimestamp <= domainParameters
        .decisionTimeFor(requestTimestamp)
        .valueOr(e =>
          throw new IllegalStateException(s"Cannot enforce decision time constraint: $e")
        ),
      withRc(rc, "Result message after decision time"),
    )

    for {
      _ <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addResult(rc, sc, resultTimestamp, commitTime))
        .leftMap(e => {
          SyncServiceAlarm.Warn(s"Failed to add result for $requestId. $e").report()
          e
        })
      commitSetF = commitSetOF.getOrElse(Future.successful(CommitSet.empty))
      commitSetT <- EitherT.right(commitSetF.transform(Success(_)))
      commitFuture <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addCommitSet(rc, commitSetT))
        .leftMap(e => {
          SyncServiceAlarm
            .Warn(s"Unexpected confirmation result message for $requestId. $e")
            .report()
          e: RequestTracker.RequestTrackerError
        })
    } yield {
      commitFuture
        .valueOr(e =>
          SyncServiceAlarm
            .Warn(withRc(rc, s"An error occurred while persisting commit set: $e"))
            .report()
        )
        .flatMap(_ => FutureUnlessShutdown.fromTry(commitSetT))
    }
  }

  private def handleTimeout(
      requestId: RequestId,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      decisionTime: CantonTimestamp,
      timeoutEvent: => Either[steps.ResultError, Option[TimestampedEvent]],
  )(
      result: TimeoutResult
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, steps.ResultError, Unit] =
    if (result.timedOut) {
      logger.info(
        show"${steps.requestKind.unquoted} request at $requestId timed out without a transaction result message."
      )

      def publishEvent(): EitherT[Future, steps.ResultError, Unit] = {
        for {
          maybeEvent <- EitherT.fromEither[Future](timeoutEvent)
          _ <- EitherT.liftF(
            ephemeral.recordOrderPublisher
              .schedulePublication(
                sequencerCounter,
                requestCounter,
                requestId.unwrap,
                maybeEvent,
              )
          )
          requestTimestamp = requestId.unwrap
          _unit <- EitherT.right[steps.ResultError](
            terminateRequest(requestCounter, sequencerCounter, requestTimestamp, decisionTime)
          )
        } yield ()
      }

      for {
        pendingRequestDataOrReplayData <- EitherT.liftF(
          ephemeral.phase37Synchronizer
            .awaitConfirmed(steps.requestType)(requestId)
            .map {
              case RequestOutcome.Success(pendingRequestData) =>
                // If the request has timed out (past its decision time), we will not need the result of the engine
                // computation, and we can therefore abort. If the computation has already completed, this will have no effect.
                pendingRequestData.abortEngine(s"request $requestId has timed out")
                pendingRequestData
              case RequestOutcome.AlreadyServedOrTimeout =>
                throw new IllegalStateException(s"Unknown pending request $requestId at timeout.")
              case RequestOutcome.Invalid =>
                throw new IllegalStateException(s"Invalid pending request $requestId.")
            }
        )

        // No need to clean up the pending submissions because this is handled (concurrently) by schedulePendingSubmissionRemoval
        cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)

        _ <- ifThenET(!cleanReplay)(publishEvent()).mapK(FutureUnlessShutdown.outcomeK)
      } yield ()
    } else EitherT.pure[FutureUnlessShutdown, steps.ResultError](())

  private[this] def isCleanReplay(
      requestCounter: RequestCounter,
      pendingData: PendingRequestData,
  ): Boolean = {
    val cleanReplay = isCleanReplay(requestCounter)
    if (cleanReplay != pendingData.isCleanReplay)
      throw new IllegalStateException(
        s"Request $requestCounter is before the starting point at ${ephemeral.startingPoints.processing.nextRequestCounter}, but not a replay"
      )
    cleanReplay
  }

  /** A request precedes the clean replay if it came before the
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState.startingPoints]]'s
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralStateFactory.StartingPoints.cleanReplay]].
    */
  private[this] def precedesCleanReplay(requestId: RequestId): Boolean =
    requestId.unwrap <= ephemeral.startingPoints.cleanReplay.prenextTimestamp
}

object ProtocolProcessor {
  private val approvalContradictionCheckIsEnabled = new AtomicReference[Boolean](true)
  private val testsAllowedToDisableApprovalContradictionCheck = Seq(
    "LedgerAuthorizationReferenceIntegrationTestDefault",
    "LedgerAuthorizationBftOrderingIntegrationTestDefault",
    "PackageVettingIntegrationTestDefault",
  )

  private[protocol] def isApprovalContradictionCheckEnabled(loggerName: String): Boolean = {
    val checkIsEnabled = approvalContradictionCheckIsEnabled.get()

    // Ensure check is enabled except for tests allowed to disable it
    checkIsEnabled || !testsAllowedToDisableApprovalContradictionCheck.exists(loggerName.startsWith)
  }

  @VisibleForTesting
  def withApprovalContradictionCheckDisabled[A](
      loggerFactory: NamedLoggerFactory
  )(body: => A): A = {
    // Limit disabling the checks to specific tests
    require(
      testsAllowedToDisableApprovalContradictionCheck.exists(loggerFactory.name.startsWith),
      "The approval contradiction check can only be disabled for some specific tests",
    )

    val logger = loggerFactory.getLogger(this.getClass)

    blocking {
      synchronized {
        logger.info("Disabling approval contradiction check")
        approvalContradictionCheckIsEnabled.set(false)
        try {
          body
        } finally {
          approvalContradictionCheckIsEnabled.set(true)
          logger.info("Re-enabling approval contradiction check")
        }
      }
    }
  }

  sealed trait ProcessorError extends Product with Serializable with PrettyPrinting

  sealed trait SubmissionProcessingError extends ProcessorError

  sealed trait RequestProcessingError extends ProcessorError

  sealed trait ResultProcessingError extends ProcessorError

  /** We were unable to send the request to the sequencer */
  final case class SequencerRequestError(sendError: SendAsyncClientError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerRequestError] = prettyOfParam(_.sendError)
  }

  /** The sequencer refused to sequence the batch for delivery */
  final case class SequencerDeliverError(deliverError: DeliverError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerDeliverError] = prettyOfParam(_.deliverError)
  }

  /** The identity snapshot does not list a mediator, so we cannot pick one. */
  final case class NoMediatorError(topologySnapshotTimestamp: CantonTimestamp)
      extends SubmissionProcessingError {
    override def pretty: Pretty[NoMediatorError] = prettyOfClass(
      param("topology snapshot timestamp", _.topologySnapshotTimestamp)
    )
  }

  /** The sequencer did not sequence our event within the allotted time
    * @param timestamp sequencer time when the timeout occurred
    */
  final case class SequencerTimeoutError(timestamp: CantonTimestamp)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerTimeoutError] = prettyOfClass(unnamedParam(_.timestamp))
  }

  final case class UnableToGetDynamicDomainParameters(domainId: DomainId, ts: CantonTimestamp)
      extends RequestProcessingError
      with ResultProcessingError {
    override def pretty: Pretty[UnableToGetDynamicDomainParameters] = prettyOfClass(
      param("domain id", _.domainId),
      param("timestamp", _.ts),
    )
  }

  final case class RequestTrackerError(error: RequestTracker.RequestTrackerError)
      extends RequestProcessingError
      with ResultProcessingError {
    override def pretty: Pretty[RequestTrackerError] = prettyOfParam(_.error)
  }

  final case class ContractStoreError(error: NonEmptyChain[store.ContractStoreError])
      extends ResultProcessingError {
    override def pretty: Pretty[ContractStoreError] = prettyOfParam(_.error.toChain.toList)
  }

  final case class DecisionTimeElapsed(requestId: RequestId, timestamp: CantonTimestamp)
      extends ResultProcessingError {
    override def pretty: Pretty[DecisionTimeElapsed] = prettyOfClass(
      param("request id", _.requestId),
      param("timestamp", _.timestamp),
    )
  }

  final case class UnknownPendingRequest(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[UnknownPendingRequest] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class InvalidPendingRequest(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[InvalidPendingRequest] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class TimeoutResultTooEarly(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[TimeoutResultTooEarly] = prettyOfClass(unnamedParam(_.requestId))
  }

  final case class DomainParametersError(domainId: DomainId, context: String)
      extends ProcessorError {
    override def pretty: Pretty[DomainParametersError] = prettyOfClass(
      param("domain", _.domainId),
      param("context", _.context.unquoted),
    )
  }

  sealed trait MalformedPayload extends Product with Serializable with PrettyPrinting

  final case class ViewMessageError[VT <: ViewType](
      error: EncryptedViewMessageError
  ) extends MalformedPayload {
    override def pretty: Pretty[ViewMessageError.this.type] = prettyOfParam(_.error)
  }

  final case class WrongRootHash(viewTree: ViewTree, expectedRootHash: RootHash)
      extends MalformedPayload {
    override def pretty: Pretty[WrongRootHash] = prettyOfClass(
      param("view tree", _.viewTree),
      param("expected root hash", _.expectedRootHash),
    )
  }

  final case class WrongRecipients(viewTree: ViewTree) extends MalformedPayload {

    override def pretty: Pretty[WrongRecipients] =
      prettyOfClass(
        param("viewHash", _.viewTree.viewHash),
        param("viewPosition", _.viewTree.viewPosition),
      )
  }

  final case class IncompleteLightViewTree(
      position: ViewPosition
  ) extends MalformedPayload {

    override def pretty: Pretty[IncompleteLightViewTree] =
      prettyOfClass(param("position", _.position))
  }

  final case class DuplicateLightViewTree(
      position: ViewPosition
  ) extends MalformedPayload {

    override def pretty: Pretty[DuplicateLightViewTree] =
      prettyOfClass(param("position", _.position))
  }
}
