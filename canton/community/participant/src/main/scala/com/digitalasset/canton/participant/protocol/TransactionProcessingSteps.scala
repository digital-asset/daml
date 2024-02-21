// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.error.utils.DecodedCantonError
import com.daml.lf.data.ImmArray
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  ContractAuthenticationFailed,
  DomainWithoutMediatorError,
  SequencerRequest,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
  UnknownDomain,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractLookupError,
  SerializableContractOfId,
  UnknownPackageError,
}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.{
  ErrorWithInternalConsistencyCheck,
  alertingPartyLookup,
}
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.ErrorWithSubTransaction
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithSuffixesAndMerged,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, IterableUtil}
import com.digitalasset.canton.{
  DiscardOps,
  LedgerSubmissionId,
  LfKeyResolver,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  WorkflowId,
  checked,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.PLens

import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** The transaction processor that coordinates the Canton transaction protocol.
  *
  * @param participantId    The participant id hosting the transaction processor.
  */
@nowarn("msg=dead code following this construct")
class TransactionProcessingSteps(
    domainId: DomainId,
    participantId: ParticipantId,
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    confirmationResponseFactory: TransactionConfirmationResponseFactory,
    modelConformanceChecker: ModelConformanceChecker,
    staticDomainParameters: StaticDomainParameters,
    crypto: DomainSyncCryptoClient,
    contractStore: ContractStore,
    metrics: TransactionProcessingMetrics,
    serializableContractAuthenticator: SerializableContractAuthenticator,
    authenticationValidator: AuthenticationValidator,
    authorizationValidator: AuthorizationValidator,
    internalConsistencyChecker: InternalConsistencyChecker,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      SubmissionParam,
      TransactionSubmitted,
      TransactionViewType,
      TransactionResultMessage,
      TransactionSubmissionError,
    ]
    with NamedLogging {
  private def protocolVersion = staticDomainParameters.protocolVersion

  override type SubmissionSendError = TransactionProcessor.SubmissionErrors.SequencerRequest.Error
  override type PendingSubmissions = Unit
  override type PendingSubmissionId = Unit
  override type PendingSubmissionData = Nothing

  override type SubmissionResultArgs = Unit

  override type PendingDataAndResponseArgs = TransactionProcessingSteps.PendingDataAndResponseArgs

  override type RejectionArgs = TransactionProcessingSteps.RejectionArgs

  override type RequestError = TransactionProcessorError
  override type ResultError = TransactionProcessorError

  override type RequestType = ProcessingSteps.RequestType.Transaction
  override val requestType: RequestType = ProcessingSteps.RequestType.Transaction

  override def pendingSubmissions(state: SyncDomainEphemeralState): Unit = ()

  override def requestKind: String = "Transaction"

  override def submissionDescription(param: SubmissionParam): String = {
    show"submitters ${param.submitterInfo.actAs}, command-id ${param.submitterInfo.commandId}"
  }

  override def submissionIdOfPendingRequest(pendingData: PendingTransaction): Unit = ()

  override def removePendingSubmission(
      pendingSubmissions: Unit,
      pendingSubmissionId: Unit,
  ): Option[Nothing] = None

  override def prepareSubmission(
      param: SubmissionParam,
      mediator: MediatorsOfDomain,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionSubmissionError, Submission] = {
    val SubmissionParam(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      disclosedContracts,
    ) = param

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

    EitherT.rightT[FutureUnlessShutdown, TransactionSubmissionError](tracked)
  }

  override def embedNoMediatorError(error: NoMediatorError): TransactionSubmissionError =
    DomainWithoutMediatorError.Error(error.topologySnapshotTimestamp, domainId)

  override def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransactionProcessorError, CantonTimestamp] =
    parameters.decisionTimeFor(requestTs).leftMap(DomainParametersError(parameters.domainId, _))

  override def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): (Option[ViewSubmitterMetadata], Option[SubmissionTracker.SubmissionData]) = {
    val submitterMetadataO =
      views.map(_.tree.submitterMetadata.unwrap).collectFirst { case Right(meta) => meta }
    val submissionDataForTrackerO = submitterMetadataO.map(meta =>
      SubmissionTracker.SubmissionData(meta.submittingParticipant, meta.maxSequencingTime)
    )

    (submitterMetadataO, submissionDataForTrackerO)
  }

  override def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransactionProcessorError, CantonTimestamp] = parameters
    .participantResponseDeadlineFor(requestTs)
    .leftMap(DomainParametersError(parameters.domainId, _))

  private class TrackedTransactionSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      mediator: MediatorsOfDomain,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
      contractLookup: ContractLookup,
      disclosedContracts: Map[LfContractId, SerializableContract],
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
      // but that is not supported in our current architecture as the MultiDomainEventLog assigns the global offset
      // only after the event has been inserted to the ParticipantEventLog.
      lazy val emptyDeduplicationPeriod =
        DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ZERO)

      val (error, dedupInfo): (TransactionError, DeduplicationPeriod) = failure match {
        case CommandDeduplicator.AlreadyExists(completionOffset, accepted, submissionId) =>
          CommandDeduplicationError.DuplicateCommandReject(
            changeId,
            UpstreamOffsetConvert.fromGlobalOffset(completionOffset).toHexString,
            accepted,
            submissionId,
          ) ->
            DeduplicationPeriod.DeduplicationOffset(
              // Extend the reported deduplication period to include the conflicting submission,
              // as deduplication offsets are exclusive
              UpstreamOffsetConvert.fromGlobalOffset(completionOffset.toLong - 1L)
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
                earliestOffset.toHexString,
              )
          }
          error -> emptyDeduplicationPeriod
        case CommandDeduplicator.MalformedOffset(error) =>
          CommandDeduplicationError.MalformedDeduplicationOffset.Error(
            error
          ) -> emptyDeduplicationPeriod
      }
      TransactionSubmissionTrackingData(
        submitterInfo.toCompletionInfo().copy(optDeduplicationPeriod = dedupInfo.some),
        TransactionSubmissionTrackingData.CauseWithTemplate(error),
        domainId,
        protocolVersion,
      )
    }

    override def submissionId: Option[LedgerSubmissionId] = submitterInfo.submissionId

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.liftF(
      recentSnapshot.ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion).map {
        domainParameters =>
          CantonTimestamp(transactionMeta.ledgerEffectiveTime)
            .add(domainParameters.ledgerTimeRecordTimeTolerance.unwrap)
      }
    )

    override def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[Future, SubmissionTrackingData, PreparedBatch] = {
      logger.debug("Preparing batch for transaction submission")
      val submitterInfoWithDedupPeriod =
        submitterInfo.copy(deduplicationPeriod = actualDeduplicationOffset)

      def causeWithTemplate(message: String, reason: TransactionConfirmationRequestCreationError) =
        TransactionSubmissionTrackingData.CauseWithTemplate(
          SubmissionErrors.MalformedRequest.Error(message, reason)
        )

      val result = for {
        confirmationPolicy <- EitherT(
          ConfirmationPolicy
            .choose(wfTransaction.unwrap, recentSnapshot.ipsSnapshot)
            .map(
              _.headOption
                .toRight(
                  causeWithTemplate(
                    "Incompatible Domain",
                    MalformedLfTransaction(
                      s"No confirmation policy applicable (snapshot at ${recentSnapshot.ipsSnapshot.timestamp})"
                    ),
                  )
                )
            )
        )

        _submitters <- submitterInfo.actAs
          .parTraverse(rawSubmitter =>
            EitherT
              .fromEither[Future](LfPartyId.fromString(rawSubmitter))
              .leftMap[TransactionSubmissionTrackingData.RejectionCause](msg =>
                causeWithTemplate(msg, MalformedSubmitter(rawSubmitter))
              )
          )

        lookupContractsWithDisclosed: SerializableContractOfId =
          (contractId: LfContractId) =>
            disclosedContracts
              .get(contractId)
              .map(contract =>
                EitherT.rightT[Future, TransactionTreeFactory.ContractLookupError](contract)
              )
              .getOrElse(
                TransactionTreeFactory
                  .contractInstanceLookup(contractLookup)(implicitly, implicitly)(
                    contractId
                  )
              )

        confirmationRequestTimer = metrics.protocolMessages.confirmationRequestCreation
        // Perform phase 1 of the protocol that produces a transaction confirmation request
        request <- confirmationRequestTimer.timeEitherT(
          confirmationRequestFactory
            .createConfirmationRequest(
              wfTransaction,
              confirmationPolicy,
              submitterInfoWithDedupPeriod,
              transactionMeta.workflowId.map(WorkflowId(_)),
              keyResolver,
              mediator,
              recentSnapshot,
              sessionKeyStore,
              lookupContractsWithDisclosed,
              None,
              maxSequencingTime,
              protocolVersion,
            )
            .leftMap[TransactionSubmissionTrackingData.RejectionCause] {
              case TransactionTreeFactoryError(UnknownPackageError(unknownTo)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.PackageNotVettedByRecipients.Error(unknownTo))
              case TransactionTreeFactoryError(ContractLookupError(contractId, _)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.UnknownContractDomain.Error(contractId))
              case creationError =>
                causeWithTemplate("Transaction confirmation request creation failed", creationError)
            }
        )
        _ = logger.debug(s"Generated requestUuid=${request.informeeMessage.requestUuid}")
        batch <- EitherT.right[TransactionSubmissionTrackingData.RejectionCause](
          request.asBatch(recentSnapshot.ipsSnapshot)
        )
      } yield {
        val batchSize = batch.toProtoVersioned.serializedSize
        metrics.protocolMessages.confirmationRequestSize.update(batchSize)(MetricsContext.Empty)

        val rootHash = request.rootHashMessage.rootHash

        new PreparedTransactionBatch(
          batch,
          rootHash,
          submitterInfoWithDedupPeriod.toCompletionInfo(),
        ): PreparedBatch
      }

      def mkError(
          rejectionCause: TransactionSubmissionTrackingData.RejectionCause
      ): Success[Either[SubmissionTrackingData, PreparedBatch]] = {
        val trackingData = TransactionSubmissionTrackingData(
          submitterInfoWithDedupPeriod.toCompletionInfo(),
          rejectionCause,
          domainId,
          protocolVersion,
        )
        Success(Left(trackingData))
      }

      // Make sure that we don't throw an error
      EitherT(result.value.transform {
        case Success(Right(preparedBatch)) => Success(Right(preparedBatch))
        case Success(Left(rejectionCause)) => mkError(rejectionCause)
        case Failure(PassiveInstanceException(_reason)) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.PassiveReplica.Error(
              applicationId = submitterInfo.applicationId,
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
        submitterInfo.toCompletionInfo().copy(optDeduplicationPeriod = None),
        TransactionSubmissionTrackingData.TimeoutCause,
        domainId,
        protocolVersion,
      )

    override def embedInFlightSubmissionTrackerError(
        error: InFlightSubmissionTracker.InFlightSubmissionTrackerError
    ): TransactionSubmissionError = error match {
      case SubmissionAlreadyInFlight(_newSubmission, existingSubmission) =>
        TransactionProcessor.SubmissionErrors.SubmissionAlreadyInFlight(
          changeId,
          existingSubmission.submissionId,
          existingSubmission.submissionDomain,
        )
      case UnknownDomain(domainId) =>
        TransactionRoutingError.ConfigurationErrors.SubmissionDomainNotReady.Error(domainId)
      case TimeoutTooLow(_submission, lowerBound) =>
        TransactionProcessor.SubmissionErrors.TimeoutError.Error(lowerBound)
    }

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SubmissionSendError =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def shutdownDuringInFlightRegistration: TransactionSubmissionError =
      TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.Rejection()

    override def onFailure: TransactionSubmitted = TransactionSubmitted
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
        case SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded(_)) =>
          TransactionProcessor.SubmissionErrors.DomainBackpressure.Rejection(error.toString)
        case otherSendError =>
          TransactionProcessor.SubmissionErrors.SequencerRequest.Error(otherSendError)
      }
      val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(errorCode)
      TransactionSubmissionTrackingData(
        completionInfo,
        rejectionCause,
        domainId,
        protocolVersion,
      )
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: Unit,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, SubmissionSendError, SubmissionResultArgs] =
    EitherT.pure(())

  override def createSubmissionResult(
      deliver: Deliver[Envelope[?]],
      unit: Unit,
  ): TransactionSubmitted =
    TransactionSubmitted

  // TODO(#8057) extract the decryption into a helper class that can be unit-tested.
  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, DecryptedViews] =
    metrics.protocolMessages.transactionMessageReceipt.timeEitherT {
      // even if we encounter errors, we process the good views as normal
      // such that the validation is available if the transaction confirmation request gets approved nevertheless.

      val pureCrypto = snapshot.pureCrypto

      def lightTransactionViewTreeDeserializer(
          bytes: ByteString
      ): Either[DefaultDeserializationError, LightTransactionViewTree] =
        LightTransactionViewTree
          .fromByteString((pureCrypto, protocolVersion))(bytes)
          .leftMap(err => DefaultDeserializationError(err.message))

      def decryptTree(
          vt: TransactionViewMessage,
          optRandomness: Option[SecureRandomness],
      ): EitherT[Future, EncryptedViewMessageError, LightTransactionViewTree] =
        EncryptedViewMessage.decryptFor(
          snapshot,
          sessionKeyStore,
          vt,
          participantId,
          optRandomness,
        )(
          lightTransactionViewTreeDeserializer
        )

      // To recover parallel processing to the largest possible extent, we'll associate a promise to each received
      // view. The promise gets fulfilled once the randomness for that view is computed - either directly by decryption,
      // because the participant is an informee of the view, or indirectly, because the participant is an informee on an
      // ancestor view and it has derived the view randomness using the HKDF.

      // TODO(i12911): a malicious submitter can send a bogus view whose randomness cannot be decrypted/derived,
      //  crashing the SyncDomain
      val randomnessMap =
        batch.foldLeft(Map.empty[ViewHash, Promise[SecureRandomness]]) { case (m, evt) =>
          m + (evt.protocolMessage.viewHash -> new SupervisedPromise[SecureRandomness](
            "secure-randomness",
            futureSupervisor,
          ))
        }

      def extractRandomnessFromView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Future[Unit] = {
        def completeRandomnessPromise(): Unit = {
          val message = transactionViewEnvelope.protocolMessage
          val randomnessF = EncryptedViewMessage
            .decryptRandomness(
              snapshot,
              sessionKeyStore,
              message,
              participantId,
            )
            .valueOr { e =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  s"Can't decrypt the randomness of the view with hash ${message.viewHash} " +
                    s"where I'm allegedly an informee. $e"
                )
              )
            }
          checked(randomnessMap(transactionViewEnvelope.protocolMessage.viewHash))
            .completeWith(randomnessF)
            .discard[Promise[SecureRandomness]]
        }

        if (
          transactionViewEnvelope.recipients.leafRecipients.contains(MemberRecipient(participantId))
        ) Future.successful(completeRandomnessPromise())
        else {
          // check also if participant is addressed as part of a group address
          val parties = transactionViewEnvelope.recipients.leafRecipients.collect {
            case ParticipantsOfParty(party) => party
          }
          if (parties.nonEmpty) {
            crypto.ips.currentSnapshotApproximation
              .activeParticipantsOfParties(
                parties.toSeq.map(_.toLf)
              )
              .map { partiesToParticipants =>
                val participants = partiesToParticipants.values.flatten.toSet
                if (participants.contains(participantId)) completeRandomnessPromise() else ()
              }
          } else Future.unit
        }
      }

      def deriveRandomnessForSubviews(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      )(
          subviewHashAndIndex: (ViewHash, ViewPosition.MerklePathElement)
      ): Either[EncryptedViewMessageError, Unit] = {
        val (subviewHash, index) = subviewHashAndIndex
        val info = HkdfInfo.subview(index)
        for {
          subviewRandomness <-
            pureCrypto
              .computeHkdf(
                randomness.unwrap,
                randomness.unwrap.size,
                info,
              )
              .leftMap(error => EncryptedViewMessageError.HkdfExpansionError(error))
        } yield {
          randomnessMap.get(subviewHash) match {
            case Some(promise) =>
              promise.tryComplete(Success(subviewRandomness)).discard
            case None =>
              // TODO(i12911): make sure to not approve the request
              SyncServiceAlarm
                .Warn(
                  s"View ${viewMessage.viewHash} lists a subview with hash $subviewHash, but I haven't received any views for this hash"
                )
                .report()
          }
          ()
        }
      }

      def decryptViewWithRandomness(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      ): EitherT[Future, EncryptedViewMessageError, (DecryptedView, Option[Signature])] =
        for {
          ltvt <- decryptTree(viewMessage, Some(randomness))
          _ <- EitherT.fromEither[Future](
            ltvt.subviewHashes
              .zip(TransactionSubviews.indices(ltvt.subviewHashes.length))
              .traverse(
                deriveRandomnessForSubviews(viewMessage, randomness)
              )
          )
        } yield (ltvt, viewMessage.submittingParticipantSignature)

      def decryptView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Future[
        Either[EncryptedViewMessageError, (WithRecipients[DecryptedView], Option[Signature])]
      ] = {
        for {
          _ <- extractRandomnessFromView(transactionViewEnvelope)
          randomness <- randomnessMap(transactionViewEnvelope.protocolMessage.viewHash).future
          lightViewTreeE <- decryptViewWithRandomness(
            transactionViewEnvelope.protocolMessage,
            randomness,
          ).value
        } yield lightViewTreeE.map { case (view, signature) =>
          (WithRecipients(view, transactionViewEnvelope.recipients), signature)
        }
      }

      val result = for {
        decryptionResult <- batch.toNEF.parTraverse(decryptView)
      } yield DecryptedViews(decryptionResult)
      EitherT.right(result)
    }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[(WithRecipients[DecryptedView], Option[Signature])]
  ): (Seq[(WithRecipients[FullView], Option[Signature])], Seq[MalformedPayload]) = {

    val lens = PLens[
      (WithRecipients[LightTransactionViewTree], Option[Signature]),
      (WithRecipients[FullTransactionViewTree], Option[Signature]),
      LightTransactionViewTree,
      FullTransactionViewTree,
    ](_._1.unwrap)(tvt => { case (WithRecipients(_, rec), sig) =>
      (WithRecipients(tvt, rec), sig)
    })

    val (fullViews, incompleteLightViewTrees, duplicateLightViewTrees) =
      LightTransactionViewTree.toFullViewTrees(
        lens,
        protocolVersion,
        crypto.pureCrypto,
        topLevelOnly = true,
      )(decryptedViewsWithSignatures)

    val incompleteLightViewTreeErrors = incompleteLightViewTrees.map {
      case (WithRecipients(vt, _), _) =>
        ProtocolProcessor.IncompleteLightViewTree(vt.viewPosition)
    }

    val duplicateLightViewTreeErrors = duplicateLightViewTrees.map {
      case (WithRecipients(vt, _), _) =>
        ProtocolProcessor.DuplicateLightViewTree(vt.viewPosition)
    }

    (fullViews, incompleteLightViewTreeErrors ++ duplicateLightViewTreeErrors)
  }

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      fullViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[FullView], Option[Signature])]
      ],
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
      mediator: MediatorsOfDomain,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CheckActivenessAndWritePendingContracts] = {

    val fullViewTrees = fullViewsWithSignatures.map { case (view, _) => view.unwrap }

    val policy = {
      val candidates = fullViewTrees.map(_.confirmationPolicy).toSet
      // The decryptedViews should all have the same root hash and therefore the same confirmation policy.
      // Bail out, if this is not the case.
      ErrorUtil.requireArgument(
        candidates.sizeCompare(1) == 0,
        s"Decrypted views have different confirmation policies. Bailing out... $candidates",
      )
      candidates.head1
    }
    val workflowIdO =
      IterableUtil.assertAtMostOne(fullViewTrees.forgetNE.mapFilter(_.workflowIdO), "workflow")
    val submitterMetaO =
      IterableUtil.assertAtMostOne(
        fullViewTrees.forgetNE.mapFilter(_.tree.submitterMetadata.unwrap.toOption),
        "submitterMetadata",
      )

    // TODO(i12911): check that all non-root lightweight trees can be decrypted with the expected (derived) randomness
    //   Also, check that all the view's informees received the derived randomness

    val rootViewTreesWithSignatures = fullViewsWithSignatures.map {
      case (WithRecipients(viewTree, _), signature) =>
        (viewTree, signature)
    }
    val rootViews = rootViewTreesWithSignatures.map { case (viewTree, _signature) => viewTree.view }

    val usedAndCreatedF = ExtractUsedAndCreated(
      participantId,
      staticDomainParameters,
      rootViews,
      snapshot.ipsSnapshot,
      loggerFactory,
    )

    val fut = usedAndCreatedF.map { usedAndCreated =>
      val enrichedTransaction =
        EnrichedTransaction(
          policy,
          rootViewTreesWithSignatures,
          usedAndCreated,
          workflowIdO,
          submitterMetaO,
        )

      val activenessSet = usedAndCreated.activenessSet

      CheckActivenessAndWritePendingContracts(
        activenessSet,
        PendingDataAndResponseArgs(
          enrichedTransaction,
          ts,
          malformedPayloads,
          rc,
          sc,
          snapshot,
        ),
      )
    }
    EitherT.right(fut)
  }

  def authenticateInputContracts(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    authenticateInputContractsInternal(
      pendingDataAndResponseArgs.enrichedTransaction.usedAndCreated.contracts.used
    )

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      mediator: MediatorsOfDomain,
      freshOwnTimelyTx: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {

    val PendingDataAndResponseArgs(
      enrichedTransaction,
      requestTimestamp,
      malformedPayloads,
      rc,
      sc,
      snapshot,
    ) = pendingDataAndResponseArgs

    val ipsSnapshot = snapshot.ipsSnapshot
    val requestId = RequestId(requestTimestamp)

    def checkReplayedTransaction: Option[String] =
      Option.when(!freshOwnTimelyTx)("View %s belongs to a replayed transaction")

    def doParallelChecks(enrichedTransaction: EnrichedTransaction): Future[ParallelChecksResult] = {
      val ledgerTime = enrichedTransaction.ledgerTime
      val rootViewTrees = enrichedTransaction.rootViewTreesWithSignatures.map {
        case (viewTree, _) =>
          viewTree
      }

      for {
        authenticationResult <-
          authenticationValidator.verifyViewSignatures(
            requestId,
            enrichedTransaction.rootViewTreesWithSignatures,
            snapshot,
          )

        consistencyResultE = ContractConsistencyChecker
          .assertInputContractsInPast(
            enrichedTransaction.usedAndCreated.contracts.used.toList,
            ledgerTime,
          )

        domainParameters <- ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion)

        // `tryCommonData` should never throw here because all views have the same root hash
        // which already commits to the ParticipantMetadata and CommonMetadata
        commonData = checked(tryCommonData(rootViewTrees))
        amSubmitter = enrichedTransaction.submitterMetadataO.exists(
          _.submittingParticipant == participantId
        )
        timeValidationE = TimeValidator.checkTimestamps(
          commonData,
          requestTimestamp,
          domainParameters.ledgerTimeRecordTimeTolerance,
          amSubmitter,
          logger,
        )

        replayCheckResult = if (amSubmitter) checkReplayedTransaction else None

        authorizationResult <- authorizationValidator.checkAuthorization(
          requestId,
          rootViewTrees,
          ipsSnapshot,
        )

        conformanceResultE <- modelConformanceChecker
          .check(
            rootViewTrees,
            keyResolverFor(_),
            pendingDataAndResponseArgs.rc,
            ipsSnapshot,
            commonData,
          )
          .value

        globalKeyHostedParties <- InternalConsistencyChecker.hostedGlobalKeyParties(
          rootViewTrees,
          participantId,
          snapshot.ipsSnapshot,
        )

        internalConsistencyResultE = internalConsistencyChecker.check(
          rootViewTrees,
          alertingPartyLookup(globalKeyHostedParties),
        )

      } yield ParallelChecksResult(
        authenticationResult,
        consistencyResultE,
        authorizationResult,
        conformanceResultE,
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
        if (activenessResult.inactiveTransfers.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a transaction confirmation request contains inactive transfers ${activenessResult.inactiveTransfers}"
          )
        activenessResult
    }

    def computeValidationResult(
        enrichedTransaction: EnrichedTransaction,
        parallelChecksResult: ParallelChecksResult,
        activenessResult: ActivenessResult,
    ): TransactionValidationResult = {
      val viewResults = SortedMap.newBuilder[ViewPosition, ViewValidationResult](
        ViewPosition.orderViewPosition.toOrdering
      )

      enrichedTransaction.rootViewTreesWithSignatures
        .map { case (view, _) => view }
        .forgetNE
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

      val usedAndCreated = enrichedTransaction.usedAndCreated
      validation.TransactionValidationResult(
        transactionId = enrichedTransaction.transactionId,
        confirmationPolicy = enrichedTransaction.policy,
        submitterMetadataO = enrichedTransaction.submitterMetadataO,
        workflowIdO = enrichedTransaction.workflowIdO,
        contractConsistencyResultE = parallelChecksResult.consistencyResultE,
        authenticationResult = parallelChecksResult.authenticationResult,
        authorizationResult = parallelChecksResult.authorizationResult,
        modelConformanceResultE = parallelChecksResult.conformanceResultE,
        internalConsistencyResultE = parallelChecksResult.internalConsistencyResultE,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        witnessedAndDivulged = usedAndCreated.contracts.witnessedAndDivulged,
        createdContracts = usedAndCreated.contracts.created,
        transient = usedAndCreated.contracts.transient,
        successfulActivenessCheck = activenessResult.isSuccessful,
        viewValidationResults = viewResults.result(),
        timeValidationResultE = parallelChecksResult.timeValidationResultE,
        hostedWitnesses = usedAndCreated.hostedWitnesses,
        replayCheckResult = parallelChecksResult.replayCheckResult,
      )
    }

    val mediatorRecipients = Recipients.cc(mediator)

    val result =
      for {
        parallelChecksResult <- FutureUnlessShutdown.outcomeF(doParallelChecks(enrichedTransaction))
        activenessResult <- awaitActivenessResult
        transactionValidationResult = computeValidationResult(
          enrichedTransaction,
          parallelChecksResult,
          activenessResult,
        )
        responses <- FutureUnlessShutdown.outcomeF(
          confirmationResponseFactory.createConfirmationResponses(
            requestId,
            malformedPayloads,
            transactionValidationResult,
            ipsSnapshot,
          )
        )
      } yield {

        val pendingTransaction =
          createPendingTransaction(
            requestId,
            transactionValidationResult,
            rc,
            sc,
            mediator,
            freshOwnTimelyTx,
          )
        StorePendingDataAndSendResponseAndCreateTimeout(
          pendingTransaction,
          responses.map(_ -> mediatorRecipients),
          RejectionArgs(
            pendingTransaction,
            LocalReject.TimeRejects.LocalTimeout.Reject(protocolVersion),
          ),
        )
      }
    EitherT.right(result)
  }

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Seq[ConfirmationResponse] =
    Seq(
      confirmationResponseFactory.createConfirmationResponsesForMalformedPayloads(
        requestId,
        malformedPayloads,
      )
    )

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      _rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[TimestampedEvent], Option[PendingSubmissionId]) = {
    val rejection = LedgerSyncEvent.CommandRejected.FinalReason(error.rpcStatus())

    completionInfoFromSubmitterMetadataO(submitterMetadata, freshOwnTimelyTx).map {
      completionInfo =>
        TimestampedEvent(
          LedgerSyncEvent.CommandRejected(
            ts.toLf,
            completionInfo,
            rejection,
            requestType,
            domainId,
          ),
          RequestOffset(ts, rc),
          Some(sc),
        )
    } -> None // Transaction processing doesn't use pending submissions
  }

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: Nothing,
  )(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def createRejectionEvent(rejectionArgs: TransactionProcessingSteps.RejectionArgs)(
      implicit traceContext: TraceContext
  ): Either[TransactionProcessorError, Option[TimestampedEvent]] = {

    val RejectionArgs(pendingTransaction, rejectionReason) = rejectionArgs
    val PendingTransaction(
      _,
      freshOwnTimelyTx,
      _,
      _,
      _,
      requestTime,
      requestCounter,
      requestSequencerCounter,
      transactionValidationResult,
      _,
    ) =
      pendingTransaction
    val submitterMetaO = transactionValidationResult.submitterMetadataO
    val completionInfoO =
      submitterMetaO.flatMap(completionInfoFromSubmitterMetadataO(_, freshOwnTimelyTx))

    rejectionReason.logWithContext(Map("requestId" -> pendingTransaction.requestId.toString))
    val rejectionReasonStatus = rejectionReason.rpcStatusWithoutLoggingContext()
    val mappedRejectionReason =
      DecodedCantonError.fromGrpcStatus(rejectionReasonStatus) match {
        case Right(error) => rejectionReasonStatus
        case Left(err) =>
          logger.warn(s"Failed to parse the rejection reason: $err")
          rejectionReasonStatus
      }
    val rejection = LedgerSyncEvent.CommandRejected.FinalReason(mappedRejectionReason)

    val tseO = completionInfoO.map(info =>
      TimestampedEvent(
        LedgerSyncEvent
          .CommandRejected(requestTime.toLf, info, rejection, requestType, domainId),
        RequestOffset(requestTime, requestCounter),
        Some(requestSequencerCounter),
      )
    )

    Right(tseO)
  }

  @VisibleForTesting
  private[protocol] def authenticateInputContractsInternal(
      inputContracts: Map[LfContractId, SerializableContract]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    EitherT.fromEither(
      inputContracts.toList
        .traverse_ { case (contractId, contract) =>
          serializableContractAuthenticator
            .authenticate(contract)
            .leftMap(message => ContractAuthenticationFailed.Error(contractId, message).reported())
        }
    )

  private def completionInfoFromSubmitterMetadataO(
      meta: SubmitterMetadata,
      freshOwnTimelyTx: Boolean,
  ): Option[CompletionInfo] = {
    lazy val completionInfo = CompletionInfo(
      meta.actAs.toList,
      meta.applicationId.unwrap,
      meta.commandId.unwrap,
      Some(meta.dedupPeriod),
      meta.submissionId,
      statistics = None, // Statistics filled by ReadService, so we don't persist them
    )

    Option.when(freshOwnTimelyTx)(completionInfo)
  }

  private[this] def createPendingTransaction(
      id: RequestId,
      transactionValidationResult: TransactionValidationResult,
      rc: RequestCounter,
      sc: SequencerCounter,
      mediator: MediatorsOfDomain,
      freshOwnTimelyTx: Boolean,
  ): PendingTransaction = {
    val TransactionValidationResult(
      transactionId,
      confirmationPolicies,
      submitterMetaO,
      workflowIdO,
      contractConsistencyE,
      authenticationResult,
      authorizationResult,
      modelConformanceResultE,
      internalConsistencyResultE,
      consumedInputsOfHostedParties,
      witnessedAndDivulged,
      createdContracts,
      transient,
      successfulActivenessCheck,
      viewValidationResults,
      timeValidationE,
      hostedInformeeStakeholders,
      replayCheckResult,
    ) = transactionValidationResult

    validation.PendingTransaction(
      transactionId,
      freshOwnTimelyTx,
      modelConformanceResultE,
      internalConsistencyResultE,
      workflowIdO,
      id.unwrap,
      rc,
      sc,
      transactionValidationResult,
      mediator,
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApproveConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      modelConformanceResult: ModelConformanceChecker.Result,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val txValidationResult = pendingRequestData.transactionValidationResult
    val commitSet = txValidationResult.commitSet(pendingRequestData.requestId)(protocolVersion)

    computeCommitAndContractsAndEvent(
      requestTime = pendingRequestData.requestTime,
      requestCounter = pendingRequestData.requestCounter,
      txId = pendingRequestData.txId,
      workflowIdO = pendingRequestData.workflowIdO,
      requestSequencerCounter = pendingRequestData.requestSequencerCounter,
      commitSet = commitSet,
      createdContracts = txValidationResult.createdContracts,
      witnessedAndDivulged = txValidationResult.witnessedAndDivulged,
      hostedWitnesses = txValidationResult.hostedWitnesses,
      completionInfoO = completionInfoO,
      lfTx = modelConformanceResult.suffixedTransaction,
    )
  }

  private def computeCommitAndContractsAndEvent(
      requestTime: CantonTimestamp,
      requestCounter: RequestCounter,
      txId: TransactionId,
      workflowIdO: Option[WorkflowId],
      requestSequencerCounter: SequencerCounter,
      commitSet: CommitSet,
      createdContracts: Map[LfContractId, SerializableContract],
      witnessedAndDivulged: Map[LfContractId, SerializableContract],
      hostedWitnesses: Set[LfPartyId],
      completionInfoO: Option[CompletionInfo],
      lfTx: WellFormedTransaction[WithSuffixesAndMerged],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val commitSetF = Future.successful(commitSet)
    val contractsToBeStored = createdContracts.values.toSeq.map(WithTransactionId(_, txId))

    def storeDivulgedContracts: Future[Unit] =
      contractStore
        .storeDivulgedContracts(
          requestCounter,
          witnessedAndDivulged.values.toSeq,
        )

    for {
      // Store the divulged contracts in the contract store
      _ <- EitherT.right(storeDivulgedContracts)

      lfTxId <- EitherT
        .fromEither[Future](txId.asLedgerTransactionId)
        .leftMap[TransactionProcessorError](FieldConversionError("Transaction Id", _))

      contractMetadata =
        // TODO(#11047): Forward driver contract metadata also for divulged contracts
        createdContracts.view.collect {
          case (contractId, SerializableContract(_, _, _, _, Some(salt))) =>
            contractId -> DriverContractMetadata(salt).toLfBytes(protocolVersion)
        }.toMap

      acceptedEvent = LedgerSyncEvent.TransactionAccepted(
        completionInfoO = completionInfoO,
        transactionMeta = TransactionMeta(
          ledgerEffectiveTime = lfTx.metadata.ledgerTime.toLf,
          workflowId = workflowIdO.map(_.unwrap),
          submissionTime = lfTx.metadata.submissionTime.toLf,
          // Set the submission seed to zeros one (None no longer accepted) because it is pointless for projected
          // transactions and it leaks the structure of the omitted parts of the transaction.
          submissionSeed = LedgerSyncEvent.noOpSeed,
          optUsedPackages = None,
          optNodeSeeds = Some(lfTx.metadata.seeds.to(ImmArray)),
          optByKeyNodes = None, // optByKeyNodes is unused by the indexer
        ),
        transaction = LfCommittedTransaction(lfTx.unwrap),
        transactionId = lfTxId,
        recordTime = requestTime.toLf,
        divulgedContracts = witnessedAndDivulged.map { case (divulgedCid, divulgedContract) =>
          DivulgedContract(divulgedCid, divulgedContract.contractInstance)
        }.toList,
        blindingInfoO = None,
        hostedWitnesses = hostedWitnesses.toList,
        contractMetadata = contractMetadata,
        domainId = domainId,
      )

      timestampedEvent = TimestampedEvent(
        acceptedEvent,
        RequestOffset(requestTime, requestCounter),
        Some(requestSequencerCounter),
      )
    } yield CommitAndStoreContractsAndPublishEvent(
      Some(commitSetF),
      contractsToBeStored,
      Some(timestampedEvent),
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfoO: Option[CompletionInfo],
      validSubTransaction: WellFormedTransaction[WithSuffixesAndMerged],
      validSubViewsNE: NonEmpty[Seq[TransactionView]],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    for {
      usedAndCreated <- EitherT.right(
        ExtractUsedAndCreated(
          participantId,
          staticDomainParameters,
          validSubViewsNE,
          topologySnapshot,
          loggerFactory,
        )
      )

      createdContracts = usedAndCreated.contracts.created

      commitSet = CommitSet.createForTransaction(
        successfulActivenessCheck =
          pendingRequestData.transactionValidationResult.successfulActivenessCheck,
        requestId = pendingRequestData.requestId,
        consumedInputsOfHostedParties = usedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        transient = usedAndCreated.contracts.transient,
        createdContracts = createdContracts,
      )(protocolVersion)

      commitAndContractsAndEvent <- computeCommitAndContractsAndEvent(
        requestTime = pendingRequestData.requestTime,
        requestCounter = pendingRequestData.requestCounter,
        txId = pendingRequestData.txId,
        workflowIdO = pendingRequestData.workflowIdO,
        requestSequencerCounter = pendingRequestData.requestSequencerCounter,
        commitSet = commitSet,
        createdContracts = createdContracts,
        witnessedAndDivulged = usedAndCreated.contracts.witnessedAndDivulged,
        hostedWitnesses = usedAndCreated.hostedWitnesses,
        completionInfoO = completionInfoO,
        lfTx = validSubTransaction,
      )
    } yield commitAndContractsAndEvent
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[MalformedConfirmationRequestResult, TransactionResultMessage],
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val content = eventE.fold(_.content, _.content)
    val Deliver(_, ts, _, _, _) = content
    val submitterMetaO = pendingRequestData.transactionValidationResult.submitterMetadataO
    val completionInfoO = submitterMetaO.flatMap(
      completionInfoFromSubmitterMetadataO(_, pendingRequestData.freshOwnTimelyTx)
    )

    def getCommitSetAndContractsToBeStoredAndEvent(
        topologySnapshot: TopologySnapshot
    ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
      import scala.util.Either.MergeableEither
      (
        MergeableEither[ConfirmationResult](resultE).merge.verdict,
        pendingRequestData.modelConformanceResultE,
      ) match {
        case (_: Verdict.Approve, _) => handleApprovedVerdict(topologySnapshot)
        case (_, Left(error)) => rejectedWithModelConformanceError(error)
        case (reasons: Verdict.ParticipantReject, _) =>
          rejected(reasons.keyEvent)
        case (reject: Verdict.MediatorReject, _) =>
          rejected(reject)
      }
    }

    def handleApprovedVerdict(topologySnapshot: TopologySnapshot)(implicit
        traceContext: TraceContext
    ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
      pendingRequestData.modelConformanceResultE match {
        case Right(modelConformanceResult) =>
          getCommitSetAndContractsToBeStoredAndEventApproveConform(
            pendingRequestData,
            completionInfoO,
            modelConformanceResult,
          )

        case Left(
              ErrorWithSubTransaction(
                _errors,
                Some(validSubTransaction),
                NonEmpty(validSubViewsNE),
              )
            ) =>
          getCommitSetAndContractsToBeStoredAndEventApprovePartlyConform(
            pendingRequestData,
            completionInfoO,
            validSubTransaction,
            validSubViewsNE,
            topologySnapshot,
          )

        case Left(error) =>
          // There is no valid subview
          //   -> we can reject as no participant will commit a subtransaction and violate transparency.
          rejectedWithModelConformanceError(error)
      }
    }

    def rejectedWithModelConformanceError(error: ErrorWithSubTransaction) =
      rejected(
        LocalReject.MalformedRejects.ModelConformance.Reject(error.errors.head1.toString)(
          LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)
        )
      )

    def rejected(rejection: TransactionRejection) = {
      for {
        event <- EitherT.fromEither[Future](
          createRejectionEvent(RejectionArgs(pendingRequestData, rejection))
        )
      } yield CommitAndStoreContractsAndPublishEvent(None, Seq(), event)
    }

    for {
      topologySnapshot <- EitherT.right[TransactionProcessorError](
        crypto.ips.awaitSnapshot(pendingRequestData.requestTime)
      )
      maxDecisionTime <- ProcessingSteps
        .getDecisionTime(topologySnapshot, pendingRequestData.requestTime)
        .leftMap(DomainParametersError(domainId, _))
      _ <-
        if (ts <= maxDecisionTime) EitherT.pure[Future, TransactionProcessorError](())
        else
          EitherT.right[TransactionProcessorError](
            Future.failed(new IllegalArgumentException("Timeout message after decision time"))
          )

      resultTopologySnapshot <- EitherT.right[TransactionProcessorError](
        crypto.ips.awaitSnapshot(ts)
      )
      mediatorActiveAtResultTs <- EitherT.right[TransactionProcessorError](
        resultTopologySnapshot.isMediatorActive(pendingRequestData.mediator)
      )
      res <-
        if (mediatorActiveAtResultTs) getCommitSetAndContractsToBeStoredAndEvent(topologySnapshot)
        else {
          // Additional validation requested during security audit as DIA-003-013.
          // Activeness of the mediator already gets checked in Phase 3,
          // this additional validation covers the case that the mediator gets deactivated between Phase 3 and Phase 7.
          rejected(
            LocalReject.MalformedRejects.MalformedRequest.Reject(
              s"The mediator ${pendingRequestData.mediator} has been deactivated while processing the request. Rolling back.",
              protocolVersion,
            )
          )
        }
    } yield res
  }

  override def postProcessResult(verdict: Verdict, pendingSubmission: Nothing)(implicit
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
}

object TransactionProcessingSteps {

  final case class SubmissionParam(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, SerializableContract],
  )

  final case class EnrichedTransaction(
      policy: ConfirmationPolicy,
      rootViewTreesWithSignatures: NonEmpty[Seq[(FullTransactionViewTree, Option[Signature])]],
      usedAndCreated: UsedAndCreated,
      workflowIdO: Option[WorkflowId],
      submitterMetadataO: Option[SubmitterMetadata],
  ) {

    def transactionId: TransactionId = {
      val (tvt, _) = rootViewTreesWithSignatures.head1
      tvt.transactionId
    }

    def ledgerTime: CantonTimestamp = {
      val (tvt, _) = rootViewTreesWithSignatures.head1
      tvt.ledgerTime
    }
  }

  private final case class ParallelChecksResult(
      authenticationResult: Map[ViewPosition, String],
      consistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
      authorizationResult: Map[ViewPosition, String],
      conformanceResultE: Either[
        ModelConformanceChecker.ErrorWithSubTransaction,
        ModelConformanceChecker.Result,
      ],
      internalConsistencyResultE: Either[ErrorWithInternalConsistencyCheck, Unit],
      timeValidationResultE: Either[TimeCheckFailure, Unit],
      replayCheckResult: Option[String],
  )

  final case class PendingDataAndResponseArgs(
      enrichedTransaction: EnrichedTransaction,
      requestTimestamp: CantonTimestamp,
      malformedPayloads: Seq[MalformedPayload],
      rc: RequestCounter,
      sc: SequencerCounter,
      snapshot: DomainSnapshotSyncCryptoApi,
  )

  final case class RejectionArgs(
      pendingTransaction: PendingTransaction,
      error: TransactionRejection,
  )

  def keyResolverFor(
      rootView: TransactionView
  )(implicit loggingContext: NamedLoggingContext): LfKeyResolver =
    rootView.globalKeyInputs.fmap(_.resolution)

  /** @throws java.lang.IllegalArgumentException if `receivedViewTrees` contains views with different transaction root hashes
    */
  def tryCommonData(receivedViewTrees: NonEmpty[Seq[FullTransactionViewTree]]): CommonData = {
    val distinctCommonData = receivedViewTrees
      .map(v => CommonData(v.transactionId, v.ledgerTime, v.submissionTime, v.confirmationPolicy))
      .distinct
    if (distinctCommonData.lengthCompare(1) == 0) distinctCommonData.head1
    else
      throw new IllegalArgumentException(
        s"Found several different transaction IDs, LETs or confirmation policies: $distinctCommonData"
      )
  }

  final case class CommonData(
      transactionId: TransactionId,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      confirmationPolicy: ConfirmationPolicy,
  )
}
