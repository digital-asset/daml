// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.LightTransactionViewTree.ToFullViewTreesResult
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.ledger.participant.state.Update.ContractInfo
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.SameAsContractPackageId
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{DecryptedViews, ParsedRequest}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  ViewMessageError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  SequencerRequest,
  SubmissionDuringShutdown,
  SubmissionInternalError,
  SynchronizerWithoutMediatorError,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractInstanceOfId,
  ContractLookupError,
  UnknownPackageError,
}
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator.AuthenticationValidatorResult
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.{
  ErrorWithSubTransaction,
  LazyAsyncReInterpretationMap,
}
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.util.DAMLe.{ContractEnricher, TransactionEnricher}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.ContractIdAbsolutizationDataV1
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.InvalidContractIdInView
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.{ConfirmationRequestSessionKeyStore, SessionKeyStore}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, LoggerUtil, RoseTree}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LedgerSubmissionId,
  LfKeyResolver,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  WorkflowId,
  checked,
}
import com.digitalasset.daml.lf.transaction.CreationTime
import monocle.PLens

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** The transaction processor that coordinates the Canton transaction protocol.
  *
  * @param participantId
  *   The participant id hosting the transaction processor.
  */
class TransactionProcessingSteps(
    psid: PhysicalSynchronizerId,
    participantId: ParticipantId,
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    confirmationResponsesFactory: TransactionConfirmationResponsesFactory,
    modelConformanceChecker: ModelConformanceChecker,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    crypto: SynchronizerCryptoClient,
    metrics: TransactionProcessingMetrics,
    transactionEnricher: TransactionEnricher,
    createNodeEnricher: ContractEnricher,
    authorizationValidator: AuthorizationValidator,
    internalConsistencyChecker: InternalConsistencyChecker,
    tracker: CommandProgressTracker,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    messagePayloadLoggingEnabled: Boolean,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      SubmissionParam,
      TransactionSubmissionResult,
      TransactionViewType,
      TransactionSubmissionError,
    ]
    with NamedLogging {
  private def protocolVersion = staticSynchronizerParameters.protocolVersion

  override type SubmissionSendError = TransactionProcessor.SubmissionErrors.SequencerRequest.Error
  override type PendingSubmissions = Unit
  override type PendingSubmissionId = Unit
  override type PendingSubmissionData = None.type

  override type ViewAbsoluteLedgerEffects = LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect

  /** The rose tree of the subviews' [[ViewAbsoluteLedgerEffects]] following the view structure */
  override type FullViewAbsoluteLedgerEffects = RoseTree[ViewAbsoluteLedgerEffects]

  override type ParsedRequestType = ParsedTransactionRequest

  override type RejectionArgs = TransactionProcessingSteps.RejectionArgs

  override type RequestError = TransactionProcessorError
  override type ResultError = TransactionProcessorError

  override type RequestType = ProcessingSteps.RequestType.Transaction
  override val requestType: RequestType = ProcessingSteps.RequestType.Transaction

  override def pendingSubmissions(state: SyncEphemeralState): Unit = ()

  override def requestKind: String = "Transaction"

  override def submissionDescription(param: SubmissionParam): String =
    show"submitters ${param.submitterInfo.actAs}, command-id ${param.submitterInfo.commandId}"

  override def explicitMediatorGroup(param: SubmissionParam): Option[MediatorGroupIndex] =
    param.submitterInfo.externallySignedSubmission.map(_.mediatorGroup)

  override def submissionIdOfPendingRequest(pendingData: PendingTransaction): Unit = ()

  override def removePendingSubmission(
      pendingSubmissions: Unit,
      pendingSubmissionId: Unit,
  ): Option[Nothing] = None

  override def setDecisionTimeTickRequest(
      pendingSubmissionData: None.type,
      requestedTick: SynchronizerTimeTracker.TickRequest,
  ): Unit = ()

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralState,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionSubmissionError,
    (Submission, PendingSubmissionData),
  ] = {
    val SubmissionParam(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      disclosedContracts,
    ) = submissionParam

    val tracked = new TrackedTransactionSubmission(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      mediator,
      recentSnapshot,
      ephemeralState.contractLookup,
      disclosedContracts,
    )

    EitherT.rightT[FutureUnlessShutdown, TransactionSubmissionError]((tracked, None))
  }

  override def embedNoMediatorError(error: NoMediatorError): TransactionSubmissionError =
    SynchronizerWithoutMediatorError.Error(error.topologySnapshotTimestamp, psid)

  override def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): Option[ViewSubmitterMetadata] =
    views.map(_.tree.submitterMetadata.unwrap).collectFirst { case Right(meta) => meta }

  private class TrackedTransactionSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      mediator: MediatorGroupRecipient,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
      contractLookup: ContractLookup { type ContractsCreatedAtTime <: CreationTime.CreatedAt },
      disclosedContracts: Map[LfContractId, ContractInstance],
  )(implicit traceContext: TraceContext)
      extends TrackedSubmission {

    private def changeId: ChangeId = submitterInfo.changeId

    override val changeIdHash: ChangeIdHash = ChangeIdHash(changeId)

    override def specifiedDeduplicationPeriod: DeduplicationPeriod =
      submitterInfo.deduplicationPeriod

    override def commandDeduplicationFailure(
        failure: DeduplicationFailed
    ): TransactionSubmissionTrackingData = {
      // If the deduplication period is not supported, we report the empty deduplication period to be on the safe side
      // Ideally, we'd report the offset that is being assigned to the completion event,
      // but that is not supported in our current architecture as the indexer assigns the global offset at a later stage
      // of processing.
      lazy val emptyDeduplicationPeriod =
        DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ZERO)

      val (error, dedupInfo): (TransactionError, DeduplicationPeriod) = failure match {
        case CommandDeduplicator.AlreadyExists(completionOffset, accepted, submissionId) =>
          CommandDeduplicationError.DuplicateCommandReject(
            changeId,
            completionOffset.unwrap,
            accepted,
            submissionId,
          ) ->
            DeduplicationPeriod.DeduplicationOffset(
              // Extend the reported deduplication period to include the conflicting submission,
              // as deduplication offsets are exclusive
              Option.unless(completionOffset.unwrap - 1L == 0)(
                Offset.tryFromLong(completionOffset.unwrap - 1L)
              )
            )
        case CommandDeduplicator.DeduplicationPeriodTooEarly(requested, supported) =>
          val error: TransactionError = supported match {
            case DeduplicationPeriod.DeduplicationDuration(longestDuration) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithDuration(
                changeId,
                requested,
                longestDuration.toString,
              )
            case DeduplicationPeriod.DeduplicationOffset(earliestOffset) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithOffset(
                changeId,
                requested,
                earliestOffset.fold(0L)(_.unwrap),
              )
          }
          error -> emptyDeduplicationPeriod
      }
      mkTransactionSubmissionTrackingData(
        error,
        submitterInfo.toCompletionInfo.copy(optDeduplicationPeriod = dedupInfo.some),
      )
    }

    private def mkTransactionSubmissionTrackingData(
        error: TransactionError,
        completionInfo: CompletionInfo,
    ): TransactionSubmissionTrackingData =
      TransactionSubmissionTrackingData(
        completionInfo,
        TransactionSubmissionTrackingData.CauseWithTemplate(error),
        psid,
      )

    override def submissionId: Option[LedgerSubmissionId] = submitterInfo.submissionId

    override def maxSequencingTimeO: OptionT[FutureUnlessShutdown, CantonTimestamp] = OptionT.liftF(
      recentSnapshot.ipsSnapshot.findDynamicSynchronizerParametersOrDefault(protocolVersion).map {
        synchronizerParameters =>
          val maxSequencingTimeFromLET = CantonTimestamp(transactionMeta.ledgerEffectiveTime)
            .add(synchronizerParameters.ledgerTimeRecordTimeTolerance.unwrap)
          // For PV34 we didn't want to change the protocol so used adjusted the max sequencing time
          // not to exceed the max record time if provided. For PV35 onwards we do this checking in phase 3
          // so we restore the pre-existing.
          if (protocolVersion >= ProtocolVersion.v35) {
            maxSequencingTimeFromLET
          } else {
            submitterInfo.externallySignedSubmission
              .flatMap(_.maxRecordTime)
              .map(CantonTimestamp.apply)
              .map(_.min(maxSequencingTimeFromLET))
              .getOrElse(maxSequencingTimeFromLET)
          }
      }
    )

    override def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[FutureUnlessShutdown, SubmissionTrackingData, PreparedBatch] = {
      logger.debug("Preparing batch for transaction submission")
      val submitterInfoWithDedupPeriod =
        submitterInfo.copy(deduplicationPeriod = actualDeduplicationOffset)

      def causeWithTemplate(
          message: String,
          reason: TransactionConfirmationRequestCreationError,
      ): TransactionSubmissionTrackingData.CauseWithTemplate =
        TransactionSubmissionTrackingData.CauseWithTemplate(
          SubmissionErrors.MalformedRequest.Error(message, reason)
        )

      val result = for {
        _ <- submitterInfo.actAs
          .parTraverse(rawSubmitter =>
            EitherT
              .fromEither[FutureUnlessShutdown](LfPartyId.fromString(rawSubmitter))
              .leftMap[TransactionSubmissionTrackingData.RejectionCause](msg =>
                causeWithTemplate(msg, MalformedSubmitter(rawSubmitter))
              )
          )

        confirmationRequestTimer = metrics.protocolMessages.confirmationRequestCreation
        // Perform phase 1 of the protocol that produces a transaction confirmation request
        request <- confirmationRequestTimer.timeEitherFUS(
          confirmationRequestFactory
            .createConfirmationRequest(
              wfTransaction,
              submitterInfoWithDedupPeriod,
              transactionMeta.workflowId.map(WorkflowId(_)),
              keyResolver,
              mediator,
              recentSnapshot,
              sessionKeyStore,
              TransactionProcessingSteps
                .lookupContractsWithDisclosed(disclosedContracts, contractLookup),
              maxSequencingTime,
              protocolVersion,
            )
            .leftMap[TransactionSubmissionTrackingData.RejectionCause] {
              case TransactionTreeFactoryError(UnknownPackageError(unknownTo)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.PackageNotVettedByRecipients.Error(unknownTo))
              case TransactionTreeFactoryError(ContractLookupError(contractId, _)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.UnknownContractSynchronizer.Error(contractId))
              case creationError =>
                causeWithTemplate("Transaction confirmation request creation failed", creationError)
            }
        )
        _ = logger.debug(s"Generated requestUuid=${request.informeeMessage.requestUuid}")
        batch <- EitherT
          .right[TransactionSubmissionTrackingData.RejectionCause](
            request.asBatch(recentSnapshot.ipsSnapshot)
          )
      } yield {
        val batchSize = batch.toProtoVersioned.serializedSize
        val numRecipients = batch.allRecipients.size
        val numEnvelopes = batch.envelopesCount
        tracker
          .findHandle(
            submitterInfoWithDedupPeriod.commandId,
            submitterInfoWithDedupPeriod.userId,
            submitterInfoWithDedupPeriod.actAs,
            submitterInfoWithDedupPeriod.submissionId,
          )
          .recordEnvelopeSizes(batchSize, numRecipients, numEnvelopes)

        metrics.protocolMessages.confirmationRequestSize.update(batchSize)(MetricsContext.Empty)

        new PreparedTransactionBatch(
          batch,
          request.rootHash,
          submitterInfoWithDedupPeriod.toCompletionInfo,
        ): PreparedBatch
      }

      def mkError(
          rejectionCause: TransactionSubmissionTrackingData.RejectionCause
      ): Success[Outcome[Either[SubmissionTrackingData, PreparedBatch]]] = {
        val trackingData = TransactionSubmissionTrackingData(
          submitterInfoWithDedupPeriod.toCompletionInfo,
          rejectionCause,
          psid,
        )
        Success(Outcome(Left(trackingData)))
      }

      // Make sure that we don't throw an error
      EitherT(result.value.transform {
        case Success(Outcome(Right(preparedBatch))) => Success(Outcome(Right(preparedBatch)))
        case Success(Outcome(Left(rejectionCause))) => mkError(rejectionCause)
        case Success(AbortedDueToShutdown) => Success(AbortedDueToShutdown)
        case Failure(PassiveInstanceException(_reason)) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.PassiveReplica.Error(
              userId = submitterInfo.userId,
              commandId = submitterInfo.commandId,
            )
          )
          mkError(rejectionCause)
        case Failure(exception) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.InjectionFailure.Failure(exception)
          )
          mkError(rejectionCause)
      })
    }

    override def submissionTimeoutTrackingData: SubmissionTrackingData =
      TransactionSubmissionTrackingData(
        submitterInfo.toCompletionInfo.copy(optDeduplicationPeriod = None),
        TransactionSubmissionTrackingData.TimeoutCause,
        psid,
      )

    override def embedInFlightSubmissionTrackerError(
        error: InFlightSubmissionTracker.InFlightSubmissionTrackerError
    ): TransactionSubmissionError = error match {
      case SubmissionAlreadyInFlight(_newSubmission, existingSubmission) =>
        TransactionProcessor.SubmissionErrors.SubmissionAlreadyInFlight(
          changeId,
          existingSubmission.submissionId,
          existingSubmission.submissionSynchronizerId,
        )
      case TimeoutTooLow(_submission, _lowerBound) =>
        TransactionProcessor.SubmissionErrors.TimeoutError.Error()
    }

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SubmissionSendError =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def shutdownDuringInFlightRegistration: TransactionSubmissionError =
      TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.Rejection()

    override def onDefinitiveFailure: TransactionSubmissionResult = TransactionSubmissionFailure

    override def definiteFailureTrackingData(
        failure: UnlessShutdown[Throwable]
    ): SubmissionTrackingData = {
      val error = (failure match {
        case UnlessShutdown.AbortedDueToShutdown =>
          SubmissionDuringShutdown.Rejection()
        case UnlessShutdown.Outcome(exception) =>
          SubmissionInternalError.Failure(exception)
      }): TransactionError
      mkTransactionSubmissionTrackingData(error, submitterInfo.toCompletionInfo)
    }

    override def onPotentialFailure(
        maxSequencingTime: CantonTimestamp
    ): TransactionSubmissionResult =
      TransactionSubmissionUnknown(maxSequencingTime)
  }

  private class PreparedTransactionBatch(
      override val batch: Batch[DefaultOpenEnvelope],
      override val rootHash: RootHash,
      completionInfo: CompletionInfo,
  ) extends PreparedBatch {
    override def pendingSubmissionId: Unit = ()

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SequencerRequest.Error =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def submissionErrorTrackingData(
        error: SubmissionSendError
    )(implicit traceContext: TraceContext): TransactionSubmissionTrackingData = {
      val errorCode: TransactionError = error.sendError match {
        case refused @ SendAsyncClientError.RequestRefused(error) =>
          if (error.isOverload)
            TransactionProcessor.SubmissionErrors.SequencerBackpressure.Rejection(error.toString)
          else if (error.hasMaxSequencingTimeElapsed)
            TransactionProcessor.SubmissionErrors.TimeoutError.Error()
          else TransactionProcessor.SubmissionErrors.SequencerRequest.Error(refused)
        case otherSendError =>
          TransactionProcessor.SubmissionErrors.SequencerRequest.Error(otherSendError)
      }
      val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(errorCode)
      TransactionSubmissionTrackingData(
        completionInfo,
        rejectionCause,
        psid,
      )
    }

    override def logSubmissionSendError(error: SequencerRequest.Error)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Unit = {
      val logLevel = SendAsyncClientError.logLevel(error.sendError)
      LoggerUtil.logAtLevel(logLevel, s"Failed to submit transaction due to $error")
    }
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[?]],
      pendingSubmissionData: None.type,
  ): TransactionSubmitted =
    TransactionSubmitted

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[DecryptedView]] =
    metrics.protocolMessages.transactionMessageReceipt.timeEitherFUS {
      new ViewMessageDecrypter(
        participantId,
        protocolVersion,
        sessionKeyStore,
        snapshot,
        futureSupervisor,
        loggerFactory,
      ).decryptViews(batch)
    }

  override def absolutizeLedgerEffects(
      viewsWithCorrectRootHashAndRecipientsAndSignature: Seq[
        (WithRecipients[DecryptedView], Option[Signature])
      ]
  ): (
      Seq[(WithRecipients[DecryptedView], Option[Signature], ViewAbsoluteLedgerEffect)],
      Seq[MalformedPayload],
  ) =
    NonEmpty.from(viewsWithCorrectRootHashAndRecipientsAndSignature) match {
      case Some(viewsNE) =>
        // All views have the same root hash, so we can take the first one
        val firstView = viewsNE.head1._1.unwrap
        val updateId = firstView.updateId
        val ledgerTime = firstView.ledgerTime
        // TODO(#23971) Generate absolutization data based on the protocol version
        val absolutizationData = {
          updateId.discard
          ledgerTime.discard
          ContractIdAbsolutizationDataV1
        }
        val contractAbsolutizer = new ContractIdAbsolutizer(crypto.pureCrypto, absolutizationData)
        val absolutizer = new LedgerEffectAbsolutizer(contractAbsolutizer)

        viewsNE.partitionMap { case (withRecipients @ WithRecipients(view, _), sig) =>
          val vpd = view.viewParticipantData
          absolutizer
            .absoluteViewEffects(vpd, view.informees)
            .bimap(
              err =>
                ViewMessageError(
                  InvalidContractIdInView(
                    s"Failed to absolutize view at position ${view.viewPosition}: $err"
                  )
                ),
              effects => (withRecipients, sig, effects),
            )
            .swap
        }
      case None => (Seq.empty, Seq.empty)
    }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[
        (WithRecipients[DecryptedView], Option[Signature], ViewAbsoluteLedgerEffects)
      ]
  ): (
      Seq[(WithRecipients[FullView], Option[Signature], FullViewAbsoluteLedgerEffects)],
      Seq[MalformedPayload],
  ) = {

    val lens = PLens[
      (WithRecipients[LightTransactionViewTree], Option[Signature], ViewAbsoluteLedgerEffects),
      (WithRecipients[FullTransactionViewTree], Option[Signature], FullViewAbsoluteLedgerEffects),
      (LightTransactionViewTree, ViewAbsoluteLedgerEffects),
      (FullTransactionViewTree, FullViewAbsoluteLedgerEffects),
    ] { case (viewWithRecipients, _, effect) => (viewWithRecipients.unwrap, effect) }(
      tvtAndEffects => { case (WithRecipients(_, rec), sig, _) =>
        val (tvt, effects) = tvtAndEffects
        (WithRecipients(tvt, rec), sig, effects)
      }
    )

    val ToFullViewTreesResult(fullViews, incompleteLightViewTrees, duplicateLightViewTrees) =
      LightTransactionViewTree.toFullViewTrees(
        lens,
        protocolVersion,
        crypto.pureCrypto,
        topLevelOnly = true,
        decryptedViewsWithSignatures,
      )

    val incompleteLightViewTreeErrors = incompleteLightViewTrees.map {
      case (WithRecipients(vt, _), _, _) =>
        ProtocolProcessor.IncompleteLightViewTree(vt.viewPosition)
    }

    val duplicateLightViewTreeErrors = duplicateLightViewTrees.map {
      case (WithRecipients(vt, _), _, _) =>
        ProtocolProcessor.DuplicateLightViewTree(vt.viewPosition)
    }

    (fullViews, incompleteLightViewTreeErrors ++ duplicateLightViewTreeErrors)
  }

  override def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[
        Seq[
          (
              WithRecipients[FullTransactionViewTree],
              Option[Signature],
              FullViewAbsoluteLedgerEffects,
          )
        ]
      ],
      submitterMetadataO: Option[SubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ParsedTransactionRequest] = {
    val workflowId = rootViewsWithMetadata.head1._1.unwrap.workflowIdO
    val effects = rootViewsWithMetadata.forgetNE.flatMap { case (_, _, effects) =>
      effects.preorder.toVector
    }
    for {
      usedAndCreated <- ExtractUsedAndCreated(
        participantId,
        effects,
        snapshot.ipsSnapshot,
        loggerFactory,
      )
    } yield ParsedTransactionRequest(
      rc,
      ts,
      sc,
      rootViewsWithMetadata,
      submitterMetadataO,
      isFreshOwnTimelyRequest,
      malformedPayloads,
      mediator,
      usedAndCreated,
      workflowId,
      snapshot,
      synchronizerParameters,
    )
  }

  override def computeActivenessSet(
      parsedRequest: ParsedTransactionRequest
  )(implicit
      traceContext: TraceContext
  ): Either[TransactionProcessorError, ActivenessSet] =
    // TODO(i12911): check that all non-root lightweight trees can be decrypted with the expected (derived) randomness
    //   Also, check that all the view's informees received the derived randomness
    Right(parsedRequest.usedAndCreated.activenessSet)

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedTransactionRequest,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {

    val ParsedTransactionRequest(
      rc,
      requestTimestamp,
      sc,
      _,
      _,
      freshOwnTimelyTx,
      malformedPayloads,
      mediator,
      _,
      _,
      snapshot,
      _,
    ) = parsedRequest

    val ipsSnapshot = snapshot.ipsSnapshot
    val requestId = RequestId(requestTimestamp)

    def checkReplayedTransaction: Option[String] =
      Option.when(!freshOwnTimelyTx)("View %s belongs to a replayed transaction")

    def doParallelChecks(): FutureUnlessShutdown[ParallelChecksResult] = {
      val ledgerTime = parsedRequest.ledgerTime

      // Asynchronous and lazy Re-interpretation of top level views
      // This may be used by the authentication checks in case of external submissions to re-compute the externally signed
      // transaction hash.
      // Either way we also pass the result to the model conformance checker to avoid interpreting again the same transactions
      // and save us some work.
      // Note that we keep this asynchronous and lazy on purpose here, such that the authentication checks will only access the result
      // if they need to (for external submissions). For classic submissions the behavior remains the same.
      val reInterpretedTopLevelViews: LazyAsyncReInterpretationMap =
        parsedRequest.rootViewTrees.forgetNE
          .filter(_.isTopLevel)
          .map { viewTree =>
            viewTree.view.viewHash -> cats.Eval.later {
              modelConformanceChecker
                .reInterpret(
                  viewTree.view,
                  keyResolverFor(viewTree.view),
                  ledgerTime,
                  parsedRequest.preparationTime,
                  () => engineController.abortStatus,
                )
            }
          }
          .toMap

      for {
        authenticationResult <- AuthenticationValidator.verifyViewSignatures(
          parsedRequest,
          reInterpretedTopLevelViews,
          psid,
          transactionEnricher,
          createNodeEnricher,
          logger,
          messagePayloadLoggingEnabled,
        )

        consistencyResultE = ContractConsistencyChecker
          .assertInputContractsInPast(
            parsedRequest.usedAndCreated.contracts.used.toList,
            ledgerTime,
          )

        synchronizerParameters <-
          ipsSnapshot.findDynamicSynchronizerParametersOrDefault(protocolVersion)

        // `tryCommonData` should never throw here because all views have the same root hash
        // which already commits to the ParticipantMetadata and CommonMetadata
        commonData = checked(tryCommonData(parsedRequest.rootViewTrees))
        amSubmitter = parsedRequest.submitterMetadataO.exists(
          _.submittingParticipant == participantId
        )

        timeValidationE = TimeValidator.checkTimestamps(
          commonData,
          requestTimestamp,
          ledgerTimeRecordTimeTolerance = synchronizerParameters.ledgerTimeRecordTimeTolerance,
          preparationTimeRecordTimeTolerance =
            synchronizerParameters.preparationTimeRecordTimeTolerance,
          maxRecordTime = parsedRequest.submitterMetadataO
            .flatMap(_.externalAuthorization)
            .flatMap(_.maxRecordTime),
          amSubmitter = amSubmitter,
          logger,
        )

        replayCheckResult = if (amSubmitter) checkReplayedTransaction else None

        authorizationResult <- authorizationValidator.checkAuthorization(parsedRequest)

        // We run the model conformance check asynchronously so that we can complete the pending request
        // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
        // mediator verdict.
        conformanceResultET = modelConformanceChecker
          .check(
            parsedRequest.rootViewTreesWithEffects,
            keyResolverFor(_),
            ipsSnapshot,
            commonData,
            getEngineAbortStatus = () => engineController.abortStatus,
            reInterpretedTopLevelViews,
          )

        internalConsistencyResultE = internalConsistencyChecker.check(
          parsedRequest.rootViewTrees
        )

      } yield ParallelChecksResult(
        authenticationResult,
        consistencyResultE,
        authorizationResult,
        conformanceResultET,
        internalConsistencyResultE,
        timeValidationE,
        replayCheckResult,
      )
    }

    def awaitActivenessResult: FutureUnlessShutdown[ActivenessResult] = activenessResultFuture.map {
      activenessResult =>
        val contractResult = activenessResult.contracts

        if (contractResult.notFree.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a transaction confirmation request contains already non-free contracts ${contractResult.notFree}"
          )
        if (activenessResult.inactiveReassignments.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a transaction confirmation request contains inactive reassignments ${activenessResult.inactiveReassignments}"
          )
        activenessResult
    }

    def computeValidationResult(
        parsedRequest: ParsedTransactionRequest,
        parallelChecksResult: ParallelChecksResult,
        activenessResult: ActivenessResult,
    ): TransactionValidationResult = {
      val viewResults = SortedMap.newBuilder[ViewPosition, ViewValidationResult](
        ViewPosition.orderViewPosition.toOrdering
      )

      parsedRequest.rootViewTrees.forgetNE
        .flatMap(v => v.view.allSubviewsWithPosition(v.viewPosition))
        .foreach { case (view, viewPosition) =>
          val participantView = ParticipantTransactionView.tryCreate(view)
          val viewParticipantData = participantView.viewParticipantData
          val createdCore = viewParticipantData.createdCore.map(_.contract.contractId).toSet
          /* Since `viewParticipantData.coreInputs` contains all input contracts (archivals and usage only),
           * it suffices to check for `coreInputs` here.
           * We don't check for `viewParticipantData.createdInSubviewArchivedInCore` in this view
           * because it suffices to check them in the subview where the contract is created.
           */
          val coreInputs = viewParticipantData.coreInputs.keySet

          // No need to check for created contracts being locked because then they'd be reported as existing.
          val contractResult = activenessResult.contracts
          val alreadyLocked = contractResult.alreadyLocked intersect coreInputs
          val existing = contractResult.notFresh.intersect(createdCore)
          val unknown = contractResult.unknown intersect coreInputs
          val notActive = contractResult.notActive.keySet intersect coreInputs
          val inactive = unknown ++ notActive

          val viewActivenessResult = ViewActivenessResult(
            inactiveContracts = inactive,
            alreadyLockedContracts = alreadyLocked,
            existingContracts = existing,
          )

          viewResults += (viewPosition -> ViewValidationResult(
            participantView,
            viewActivenessResult,
          ))
        }

      val usedAndCreated = parsedRequest.usedAndCreated
      validation.TransactionValidationResult(
        updateId = parsedRequest.updateId,
        submitterMetadataO = parsedRequest.submitterMetadataO,
        workflowIdO = parsedRequest.workflowIdO,
        contractConsistencyResultE = parallelChecksResult.consistencyResultE,
        authenticationResult = parallelChecksResult.authenticationResult,
        authorizationResult = parallelChecksResult.authorizationResult,
        modelConformanceResultET = parallelChecksResult.conformanceResultET,
        internalConsistencyResultE = parallelChecksResult.internalConsistencyResultE,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        witnessed = usedAndCreated.contracts.witnessed,
        createdContracts = usedAndCreated.contracts.created,
        transient = usedAndCreated.contracts.transient,
        activenessResult = activenessResult,
        viewValidationResults = viewResults.result(),
        timeValidationResultE = parallelChecksResult.timeValidationResultE,
        hostedWitnesses = usedAndCreated.hostedWitnesses,
        replayCheckResult = parallelChecksResult.replayCheckResult,
        validatedExternalTransactionHash =
          parallelChecksResult.authenticationValidatorResult.externalHash,
      )
    }

    val mediatorRecipients = Recipients.cc(mediator)

    val result =
      for {
        parallelChecksResult <- doParallelChecks()
        activenessResult <- awaitActivenessResult
      } yield {
        val transactionValidationResult = computeValidationResult(
          parsedRequest,
          parallelChecksResult,
          activenessResult,
        )
        // The responses depend on the result of the model conformance check, and are therefore also delayed.
        val responsesF =
          confirmationResponsesFactory.createConfirmationResponses(
            requestId,
            malformedPayloads,
            transactionValidationResult,
            ipsSnapshot,
          )

        val pendingTransaction =
          createPendingTransaction(
            requestId,
            responsesF,
            transactionValidationResult,
            rc,
            sc,
            mediator,
            freshOwnTimelyTx,
            engineController,
            decisionTimeTickRequest,
          )
        StorePendingDataAndSendResponseAndCreateTimeout(
          pendingTransaction,
          EitherT.right(responsesF.map(_.map(_ -> mediatorRecipients))),
          RejectionArgs(
            pendingTransaction,
            ErrorDetails.fromLocalError(LocalRejectError.TimeRejects.LocalTimeout.Reject()),
          ),
        )
      }
    EitherT.right(result)
  }

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Option[ConfirmationResponses] = ProcessingSteps.constructResponsesForMalformedPayloads(
    requestId = requestId,
    rootHash = rootHash,
    malformedPayloads = malformedPayloads,
    synchronizerId = psid,
    participantId = participantId,
    protocolVersion = protocolVersion,
  )

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      _rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[SequencedUpdate], Option[PendingSubmissionId]) = {
    val rejection = Update.CommandRejected.FinalReason(error.rpcStatus())
    completionInfoFromSubmitterMetadataO(submitterMetadata, freshOwnTimelyTx).map {
      completionInfo =>
        Update.SequencedCommandRejected(
          completionInfo,
          rejection,
          psid.logical,
          ts,
        )
    } -> None // Transaction processing doesn't use pending submissions
  }

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: None.type,
  )(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def createRejectionEvent(rejectionArgs: TransactionProcessingSteps.RejectionArgs)(
      implicit traceContext: TraceContext
  ): Either[TransactionProcessorError, Option[SequencedUpdate]] = {
    val RejectionArgs(pendingTransaction, errorDetails) = rejectionArgs

    val PendingTransaction(
      freshOwnTimelyTx,
      requestTime,
      requestCounter,
      requestSequencerCounter,
      transactionValidationResult,
      _,
      _locallyRejected,
      _engineController,
      _abortedF,
      _decisionTimeTickRequest,
    ) = pendingTransaction
    val submitterMetaO = transactionValidationResult.submitterMetadataO
    val completionInfoO =
      submitterMetaO.flatMap(completionInfoFromSubmitterMetadataO(_, freshOwnTimelyTx))

    errorDetails.logRejection(
      Map("requestId" -> pendingTransaction.requestId.toString)
    )
    val rejection = Update.CommandRejected.FinalReason(errorDetails.reason)

    val updateO = completionInfoO.map(info =>
      Update.SequencedCommandRejected(
        info,
        rejection,
        psid.logical,
        requestTime,
      )
    )
    Right(updateO)
  }

  private def completionInfoFromSubmitterMetadataO(
      meta: SubmitterMetadata,
      freshOwnTimelyTx: Boolean,
  ): Option[CompletionInfo] = {
    lazy val completionInfo = CompletionInfo(
      meta.actAs.toList,
      meta.userId.unwrap,
      meta.commandId.unwrap,
      Some(meta.dedupPeriod),
      meta.submissionId,
    )

    Option.when(freshOwnTimelyTx)(completionInfo)
  }

  private[this] def createPendingTransaction(
      id: RequestId,
      responsesF: FutureUnlessShutdown[Option[ConfirmationResponses]],
      transactionValidationResult: TransactionValidationResult,
      rc: RequestCounter,
      sc: SequencerCounter,
      mediator: MediatorGroupRecipient,
      freshOwnTimelyTx: Boolean,
      engineController: EngineController,
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  )(implicit
      traceContext: TraceContext
  ): PendingTransaction = {
    // We consider that we rejected if at least one of the responses is a "reject"
    val locallyRejectedF =
      responsesF.map(_.exists(_.responses.exists(_.localVerdict.isReject)))

    // The request was aborted if the model conformance check ended with an abort error, due to either a timeout
    // or a negative mediator verdict concurrently received in Phase 7
    val engineAbortStatusF = transactionValidationResult.modelConformanceResultET.value.map {
      case Left(error) => error.engineAbortStatus
      case _ => EngineAbortStatus.notAborted
    }

    PendingTransaction(
      freshOwnTimelyTx,
      id.unwrap,
      rc,
      sc,
      transactionValidationResult,
      mediator,
      locallyRejectedF,
      engineController.abort,
      engineAbortStatusF,
      decisionTimeTickRequest,
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApproveConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      modelConformanceResult: ModelConformanceChecker.Result,
  )(implicit
      traceContext: TraceContext
  ): CommitAndStoreContractsAndPublishEvent = {
    val txValidationResult = pendingRequestData.transactionValidationResult
    val commitSet = txValidationResult.commitSet(pendingRequestData.requestId)

    computeCommitAndContractsAndEvent(
      requestTime = pendingRequestData.requestTime,
      updateId = txValidationResult.updateId,
      workflowIdO = txValidationResult.workflowIdO,
      commitSet = commitSet,
      createdContracts = txValidationResult.createdContracts,
      witnessed = txValidationResult.witnessed,
      completionInfoO = completionInfoO,
      lfTx = modelConformanceResult.suffixedTransaction,
      externalTransactionHash =
        pendingRequestData.transactionValidationResult.validatedExternalTransactionHash,
    )
  }

  // Assigning the internal contract ids to the contracts requires that all the contracts are
  // already persisted in the contract store.
  private def computeCommitAndContractsAndEvent(
      requestTime: CantonTimestamp,
      updateId: UpdateId,
      workflowIdO: Option[WorkflowId],
      commitSet: CommitSet,
      createdContracts: Map[LfContractId, NewContractInstance],
      witnessed: Map[LfContractId, GenContractInstance],
      completionInfoO: Option[CompletionInfo],
      lfTx: WellFormedTransaction[WithSuffixesAndMerged],
      externalTransactionHash: Option[Hash],
  )(implicit
      traceContext: TraceContext
  ): CommitAndStoreContractsAndPublishEvent = {
    val commitSetF = FutureUnlessShutdown.pure(commitSet)
    val ledgerEffectiveTime = lfTx.metadata.ledgerTime
    val contractsToBeStored =
      (createdContracts ++ witnessed).values
        .map(ContractInstance.assignCreationTime(_, ledgerEffectiveTime))
        .toSeq

    val contractAuthenticationData =
      // We deliberately do not forward the authentication data
      // for retroactively divulged contracts since they are not visible on the Ledger API
      // For immediately divulged contracts we populate this as those are visible.
      (createdContracts ++ witnessed).view.map { case (contractId, contract) =>
        contractId -> contract.inst.authenticationData
      }.toMap

    val acceptedEvent =
      (acsChangeFactory: AcsChangeFactory) =>
        (internalContractIds: Map[LfContractId, Long]) =>
          Update.SequencedTransactionAccepted(
            completionInfoO = completionInfoO,
            transactionMeta = TransactionMeta(
              ledgerEffectiveTime = ledgerEffectiveTime.toLf,
              workflowId = workflowIdO.map(_.unwrap),
              preparationTime = lfTx.metadata.preparationTime.toLf,
              // Set the submission seed to zeros one (None no longer accepted) because it is pointless for projected
              // transactions and it leaks the structure of the omitted parts of the transaction.
              submissionSeed = Update.noOpSeed,
              timeBoundaries = LedgerTimeBoundaries.unconstrained,
              optUsedPackages = None,
              optNodeSeeds = None, // optNodeSeeds is unused by the indexer
              optByKeyNodes = None, // optByKeyNodes is unused by the indexer
            ),
            transaction = LfCommittedTransaction(lfTx.unwrap),
            updateId = updateId,
            synchronizerId = psid.logical,
            recordTime = requestTime,
            externalTransactionHash = externalTransactionHash,
            acsChangeFactory = acsChangeFactory,
            contractInfos =
              contractAuthenticationData.map { case (contractId, contractAuthenticationData) =>
                contractId -> ContractInfo(
                  internalContractId = checked {
                    // the internal contract id must exist since we persisted the contracts before (in the ProtocolProcessor)
                    internalContractIds.getOrElse(
                      contractId,
                      ErrorUtil.invalidState(
                        s"The internal contract id for the contract $contractId was not found"
                      ),
                    )
                  },
                  contractAuthenticationData = contractAuthenticationData,
                  representativePackageId = SameAsContractPackageId,
                )
              },
          )
    CommitAndStoreContractsAndPublishEvent(
      Some(commitSetF),
      contractsToBeStored,
      Some(acceptedEvent),
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      validSubTransaction: WellFormedTransaction[WithSuffixesAndMerged],
      validSubViewEffectsNE: NonEmpty[Seq[ViewAbsoluteLedgerEffect]],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] =
    for {
      usedAndCreated <- EitherT
        .right(
          ExtractUsedAndCreated(
            participantId,
            validSubViewEffectsNE,
            topologySnapshot,
            loggerFactory,
          )
        )

      createdContracts = usedAndCreated.contracts.created

      commitSet = CommitSet.createForTransaction(
        activenessResult = pendingRequestData.transactionValidationResult.activenessResult,
        requestId = pendingRequestData.requestId,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        transient = usedAndCreated.contracts.transient,
        createdContracts = createdContracts,
      )

      commitAndContractsAndEvent = computeCommitAndContractsAndEvent(
        requestTime = pendingRequestData.requestTime,
        updateId = pendingRequestData.transactionValidationResult.updateId,
        workflowIdO = pendingRequestData.transactionValidationResult.workflowIdO,
        commitSet = commitSet,
        createdContracts = createdContracts,
        witnessed = usedAndCreated.contracts.witnessed,
        completionInfoO = completionInfoO,
        lfTx = validSubTransaction,
        externalTransactionHash =
          pendingRequestData.transactionValidationResult.validatedExternalTransactionHash,
      )
    } yield commitAndContractsAndEvent

  override def getCommitSetAndContractsToBeStoredAndEventFactory(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] = {
    val ts = event.event.content.timestamp
    val submitterMetaO = pendingRequestData.transactionValidationResult.submitterMetadataO
    val completionInfoO = submitterMetaO.flatMap(
      completionInfoFromSubmitterMetadataO(_, pendingRequestData.freshOwnTimelyTx)
    )

    def getCommitSetAndContractsToBeStoredAndEvent(
        topologySnapshot: TopologySnapshot
    ): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] = {
      val resultFE = for {
        modelConformanceResultE <-
          pendingRequestData.transactionValidationResult.modelConformanceResultET.value

        resultET = (verdict, modelConformanceResultE) match {

          // Positive verdict: we commit
          case (_: Verdict.Approve, _) => handleApprovedVerdict(topologySnapshot)

          // Model conformance check error:
          // - if the error is an abort, it means the model conformance check was still running while we received
          //   a negative verdict; we then reject with the verdict, as it is the best information we have
          // - otherwise, we reject with the actual error
          case (reasons: Verdict.ParticipantReject, Left(error)) =>
            if (error.engineAbortStatus.isAborted) {
              val errorDetails = reasons.keyErrorDetails
              rejected(errorDetails)
            } else rejectedWithModelConformanceError(error)

          case (reject: Verdict.MediatorReject, Left(error)) =>
            if (error.engineAbortStatus.isAborted)
              rejected(reject.errorDetails)
            else rejectedWithModelConformanceError(error)

          // No model conformance check error: we reject with the verdict
          case (reasons: Verdict.ParticipantReject, _) =>
            rejected(reasons.keyErrorDetails)

          case (reject: Verdict.MediatorReject, _) =>
            rejected(reject.errorDetails)

        }
        result <- resultET.value
      } yield result

      EitherT(resultFE)
    }

    def handleApprovedVerdict(topologySnapshot: TopologySnapshot)(implicit
        traceContext: TraceContext
    ): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      pendingRequestData.transactionValidationResult.modelConformanceResultET.biflatMap(
        {
          case ErrorWithSubTransaction(
                _errors,
                Some(validSubTransaction),
                NonEmpty(validSubViewEffectsNE),
              ) =>
            getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
              pendingRequestData,
              completionInfoO,
              validSubTransaction,
              validSubViewEffectsNE,
              topologySnapshot,
            )

          case error =>
            // There is no valid subview
            //   -> we can reject as no participant will commit a subtransaction and violate transparency.
            rejectedWithModelConformanceError(error)
        },
        modelConformanceResult =>
          EitherT.pure(
            getCommitSetAndContractsToBeStoredAndEventApproveConform(
              pendingRequestData,
              completionInfoO,
              modelConformanceResult,
            )
          ),
      )

    def rejectedWithModelConformanceError(error: ErrorWithSubTransaction[?]): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] = {
      val localVerdict = LocalRejectError.MalformedRejects.ModelConformance
        .Reject(error.errors.head1.toString)
      rejected(ErrorDetails(localVerdict.reason(), localVerdict.isMalformed))
    }

    def rejected(errorDetails: ErrorDetails): EitherT[
      FutureUnlessShutdown,
      TransactionProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      (for {
        eventO <- EitherT.fromEither[Future](
          createRejectionEvent(RejectionArgs(pendingRequestData, errorDetails))
        )
      } yield CommitAndStoreContractsAndPublishEvent(
        None,
        Seq(),
        eventO.map(event => _ => _ => event),
      )).mapK(FutureUnlessShutdown.outcomeK)

    for {
      topologySnapshot <- EitherT
        .right[TransactionProcessorError](
          crypto.ips.awaitSnapshot(pendingRequestData.requestTime)
        )

      maxDecisionTime <- ProcessingSteps
        .getDecisionTime(topologySnapshot, pendingRequestData.requestTime)
        .leftMap(SynchronizerParametersError(psid, _))

      _ <-
        (if (ts <= maxDecisionTime) EitherT.pure[Future, TransactionProcessorError](())
         else
           EitherT.right[TransactionProcessorError](
             Future.failed(new IllegalArgumentException("Timeout message after decision time"))
           )).mapK(FutureUnlessShutdown.outcomeK)

      resultTopologySnapshot <- EitherT
        .right[TransactionProcessorError](
          crypto.ips.awaitSnapshot(ts)
        )

      mediatorActiveAtResultTs <- EitherT
        .right[TransactionProcessorError](
          resultTopologySnapshot.isMediatorActive(pendingRequestData.mediator)
        )

      res <-
        if (mediatorActiveAtResultTs) getCommitSetAndContractsToBeStoredAndEvent(topologySnapshot)
        else {
          // Additional validation requested during security audit as DIA-003-013.
          // Activeness of the mediator already gets checked in Phase 3,
          // this additional validation covers the case that the mediator gets deactivated between Phase 3 and Phase 7.
          val localReject = LocalRejectError.MalformedRejects.MalformedRequest
            .Reject(
              s"The mediator ${pendingRequestData.mediator} has been deactivated while processing the request. Rolling back."
            )
          rejected(ErrorDetails.fromLocalError(localReject))
        }
    } yield res
  }

  override def postProcessResult(verdict: Verdict, pendingSubmission: None.type)(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)

  override def handleTimeout(parsedRequest: ParsedTransactionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, Unit] = EitherTUtil.unitUS
}

object TransactionProcessingSteps {

  private[canton] def lookupContractsWithDisclosed(
      disclosedContracts: Map[LfContractId, ContractInstance],
      contractLookup: ContractLookup { type ContractsCreatedAtTime <: CreationTime.CreatedAt },
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): ContractInstanceOfId =
    (contractId: LfContractId) =>
      disclosedContracts
        .get(contractId)
        .map(contract =>
          EitherT.rightT[FutureUnlessShutdown, TransactionTreeFactory.ContractLookupError](
            contract: GenContractInstance
          )
        )
        .getOrElse(
          TransactionTreeFactory
            .contractInstanceLookup(contractLookup)(implicitly, implicitly)(contractId)
        )

  final case class SubmissionParam(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, ContractInstance],
  )

  final case class ParsedTransactionRequest(
      override val rc: RequestCounter,
      override val requestTimestamp: CantonTimestamp,
      override val sc: SequencerCounter,
      rootViewTreesWithMetadata: NonEmpty[
        Seq[
          (
              WithRecipients[FullTransactionViewTree],
              Option[Signature],
              RoseTree[ViewAbsoluteLedgerEffect],
          )
        ]
      ],
      override val submitterMetadataO: Option[SubmitterMetadata],
      override val isFreshOwnTimelyRequest: Boolean,
      override val malformedPayloads: Seq[MalformedPayload],
      override val mediator: MediatorGroupRecipient,
      usedAndCreated: UsedAndCreated,
      workflowIdO: Option[WorkflowId],
      override val snapshot: SynchronizerSnapshotSyncCryptoApi,
      override val synchronizerParameters: DynamicSynchronizerParametersWithValidity,
  ) extends ParsedRequest[SubmitterMetadata] {

    lazy val rootViewTreesWithEffects
        : NonEmpty[Seq[(FullTransactionViewTree, RoseTree[ViewAbsoluteLedgerEffect])]] =
      rootViewTreesWithMetadata.map { case (WithRecipients(rootViewTree, _), _, effects) =>
        (rootViewTree, effects)
      }

    lazy val rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]] = rootViewTreesWithMetadata.map {
      case (WithRecipients(rootViewTree, _), _, _) => rootViewTree
    }

    lazy val rootViewTreesWithSignatures: NonEmpty[
      Seq[(FullTransactionViewTree, Option[Signature])]
    ] = rootViewTreesWithMetadata.map { case (WithRecipients(rootViewTree, _), signature, _) =>
      (rootViewTree, signature)
    }

    override def rootHash: RootHash = rootViewTrees.head1.rootHash

    def updateId: UpdateId = rootViewTrees.head1.updateId

    def ledgerTime: CantonTimestamp = rootViewTrees.head1.ledgerTime

    def preparationTime: CantonTimestamp = rootViewTrees.head1.preparationTime
  }

  private final case class ParallelChecksResult(
      authenticationValidatorResult: AuthenticationValidatorResult,
      consistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
      authorizationResult: Map[ViewPosition, String],
      conformanceResultET: EitherT[
        FutureUnlessShutdown,
        ModelConformanceChecker.ErrorWithSubTransaction[ViewAbsoluteLedgerEffect],
        ModelConformanceChecker.Result,
      ],
      internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
      timeValidationResultE: Either[TimeCheckFailure, Unit],
      replayCheckResult: Option[String],
  ) {
    val authenticationResult: Map[ViewPosition, AuthenticationError] =
      authenticationValidatorResult.viewAuthenticationErrors
  }

  final case class RejectionArgs(
      pendingTransaction: PendingTransaction,
      errorDetails: ErrorDetails,
  )

  def keyResolverFor(
      rootView: TransactionView
  )(implicit loggingContext: NamedLoggingContext): LfKeyResolver =
    rootView.globalKeyInputs.fmap(_.unversioned.resolution)

  /** @throws java.lang.IllegalArgumentException
    *   if `receivedViewTrees` contains views with different transaction root hashes
    */
  def tryCommonData(receivedViewTrees: NonEmpty[Seq[FullTransactionViewTree]]): CommonData = {
    val distinctCommonData = receivedViewTrees
      .map(v => CommonData(v.updateId, v.ledgerTime, v.preparationTime))
      .distinct
    if (distinctCommonData.lengthCompare(1) == 0) distinctCommonData.head1
    else
      throw new IllegalArgumentException(
        s"Found several different transaction IDs, LETs or confirmation policies: $distinctCommonData"
      )
  }

  final case class CommonData(
      updateId: UpdateId,
      ledgerTime: CantonTimestamp,
      preparationTime: CantonTimestamp,
  )
}
