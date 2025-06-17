// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{
  HashOps,
  Signature,
  SigningKeyUsage,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  TargetSynchronizerIsSourceSynchronizer,
  UnexpectedSynchronizer,
}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{
  ContractAuthenticator,
  EngineController,
  ProcessingSteps,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  Purged,
  ReassignedAway,
}
import com.digitalasset.canton.participant.sync.{SyncEphemeralState, SyncEphemeralStateLookup}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}

import scala.concurrent.{ExecutionContext, Future}

class UnassignmentProcessingSteps(
    val synchronizerId: Source[PhysicalSynchronizerId],
    val participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    seedGenerator: SeedGenerator,
    staticSynchronizerParameters: Source[StaticSynchronizerParameters],
    override protected val contractAuthenticator: ContractAuthenticator,
    val protocolVersion: Source[ProtocolVersion],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ReassignmentProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      UnassignmentViewType,
      PendingUnassignment,
    ]
    with NamedLogging {

  override type SubmissionResultArgs = PendingReassignmentSubmission

  override type RequestType = ProcessingSteps.RequestType.Unassignment
  override val requestType: RequestType = ProcessingSteps.RequestType.Unassignment

  override def pendingSubmissions(state: SyncEphemeralState): PendingSubmissions =
    state.pendingUnassignmentSubmissions

  override def requestKind: String = "Unassignment"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submittingParty}, contracts ${param.contractIds}, target ${param.targetSynchronizer}"

  override def explicitMediatorGroup(param: SubmissionParam): Option[MediatorGroupIndex] = None

  override def submissionIdOfPendingRequest(pendingData: PendingUnassignment): RootHash =
    pendingData.unassignmentValidationResult.rootHash

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralStateLookup,
      sourceRecentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Submission] = {
    val SubmissionParam(
      submitterMetadata,
      contractIds,
      targetSynchronizer,
    ) = submissionParam
    val pureCrypto = sourceRecentSnapshot.pureCrypto

    def withDetails(message: String) = s"unassign $contractIds to $targetSynchronizer: $message"

    val unassignmentUuid = seedGenerator.generateUuid()
    val seed = seedGenerator.generateSaltSeed()

    for {
      _ <- condUnitET[FutureUnlessShutdown](
        targetSynchronizer.unwrap != synchronizerId.unwrap,
        TargetSynchronizerIsSourceSynchronizer(synchronizerId.unwrap.logical, contractIds),
      )

      targetStaticSynchronizerParameters <- reassignmentCoordination
        .getStaticSynchronizerParameter(targetSynchronizer)
      timeProofAndSnapshot <- reassignmentCoordination
        .getTimeProofAndSnapshot(
          targetSynchronizer,
          targetStaticSynchronizerParameters,
        )
      (timeProof, targetCrypto) = timeProofAndSnapshot
      _ = logger.debug(withDetails(s"Picked time proof ${timeProof.timestamp}"))

      contractStates <- EitherT(
        ephemeralState.tracker
          .getApproximateStates(contractIds)
          .map(Right(_).withLeft[ReassignmentProcessorError])
      )

      contractCounters <- (MonadUtil.sequentialTraverse(contractIds) { contractId =>
        for {
          contract <- ephemeralState.contractLookup
            .lookup(contractId)
            .toRight[ReassignmentProcessorError](
              UnassignmentProcessorError.UnknownContract(contractId)
            )

          reassignmentCounter <- EitherT.fromEither[FutureUnlessShutdown](
            contractStates.get(contractId) match {
              case Some(state) =>
                state.status match {
                  case Active(tc) => Right(tc)
                  case Archived | Purged | _: ReassignedAway =>
                    Left(
                      UnassignmentProcessorError
                        .DeactivatedContract(contractId, status = state.status)
                    )
                }
              case None => Left(UnassignmentProcessorError.UnknownContract(contractId))
            }
          )
          newReassignmentCounter <- EitherT.fromEither[FutureUnlessShutdown](
            reassignmentCounter.increment
              .leftMap(_ =>
                (UnassignmentProcessorError.ReassignmentCounterOverflow: ReassignmentProcessorError)
              )
          )
        } yield (contract, newReassignmentCounter)
      })
      contracts <- EitherT.fromEither[FutureUnlessShutdown] {
        ContractsReassignmentBatch
          .create(contractCounters)
          .leftMap(e => ContractError(e.toString))
      }

      validated <- UnassignmentRequest
        .validated(
          participantId,
          timeProof,
          contracts,
          submitterMetadata,
          synchronizerId,
          protocolVersion,
          mediator,
          targetSynchronizer,
          Source(sourceRecentSnapshot.ipsSnapshot),
          targetCrypto.map(_.ipsSnapshot),
        )
        .leftMap(_.toSubmissionValidationError)

      fullTree = validated.request.toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        unassignmentUuid,
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- sourceRecentSnapshot
        .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
        .leftMap(ReassignmentSigningError.apply)
      mediatorMessage = fullTree.mediatorMessage(
        submittingParticipantSignature,
        staticSynchronizerParameters.map(_.protocolVersion),
      )
      maybeRecipients = Recipients.ofSet(validated.recipients)
      recipientsT <- EitherT
        .fromOption[FutureUnlessShutdown](
          maybeRecipients,
          NoStakeholders.logAndCreate(
            contracts.contractIds.toSeq,
            logger,
          ): ReassignmentProcessorError,
        )

      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          Seq(
            (
              ViewHashAndRecipients(fullTree.viewHash, recipientsT),
              None,
              fullTree.informees.toList,
            )
          ),
          parallel = true,
          pureCrypto,
          sourceRecentSnapshot,
          ephemeralState.sessionKeyStoreLookup.convertStore,
        )
        .leftMap[ReassignmentProcessorError](EncryptionError(contracts.contractIds.toSeq, _))
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
      viewMessage <- EncryptedViewMessageFactory
        .create(UnassignmentViewType)(
          fullTree,
          (viewKey, viewKeyMap),
          sourceRecentSnapshot,
          protocolVersion.unwrap,
        )
        .leftMap[ReassignmentProcessorError](EncryptionError(contracts.contractIds.toSeq, _))
      rootHashMessage =
        RootHashMessage(
          rootHash,
          synchronizerId.unwrap,
          protocolVersion.unwrap,
          ViewType.UnassignmentViewType,
          sourceRecentSnapshot.ipsSnapshot.timestamp,
          EmptyRootHashMessagePayload,
        )
      rootHashRecipients =
        Recipients.recipientGroups(
          checked(
            NonEmptyUtil.fromUnsafe(
              validated.recipients.toSeq.map(participant =>
                NonEmpty(Set, mediator, MemberRecipient(participant))
              )
            )
          )
        )

      // Each member gets a message sent to itself and to the mediator
      messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediator),
        viewMessage -> recipientsT,
        rootHashMessage -> rootHashRecipients,
      )
    } yield {
      ReassignmentsSubmission(
        Batch.of(protocolVersion.unwrap, messages*),
        rootHash,
      )
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, ReassignmentProcessorError, SubmissionResultArgs] =
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      ReassignmentRef(submissionParam.contractIds.toSet),
      submissionParam.submittingParty,
      pendingSubmissionId,
    )

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult = {
    val unassignId = UnassignId(synchronizerId.map(_.logical), deliver.timestamp)
    val reassignmentId = ReassignmentId(synchronizerId.map(_.logical), unassignId)
    SubmissionResult(reassignmentId, pendingSubmission.reassignmentCompletion.future)
  }

  override protected def decryptTree(
      sourceSnapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[UnassignmentViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageError,
    (WithRecipients[FullUnassignmentTree], Option[Signature]),
  ] =
    EncryptedViewMessage
      .decryptFor(
        sourceSnapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullUnassignmentTree
          .fromByteString(
            sourceSnapshot.pureCrypto,
            protocolVersion.map(ProtocolVersionValidation.PV(_)),
          )(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(fullTree =>
        (
          WithRecipients(fullTree, envelope.recipients),
          envelope.protocolMessage.submittingParticipantSignature,
        )
      )

  override def computeActivenessSet(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, ActivenessSet] =
    // TODO(i12926): Send a rejection if malformedPayloads is non-empty
    if (parsedRequest.fullViewTree.synchronizerId == synchronizerId.unwrap) {
      val contractIdS = parsedRequest.fullViewTree.contracts.contractIds.toSet
      // Either check contracts for activeness and lock them normally or lock them knowing the
      // contracts may not be known to the participant (e.g. due to party onboarding).
      val (checkActiveAndLock, lockMaybeUnknown) =
        if (parsedRequest.areContractsUnknown) {
          (Set.empty[LfContractId], contractIdS.forgetNE)
        } else (contractIdS.forgetNE, Set.empty[LfContractId])
      val contractsCheck = ActivenessCheck.tryCreate(
        checkFresh = Set.empty,
        checkFree = Set.empty,
        checkActive = checkActiveAndLock,
        lock = checkActiveAndLock,
        lockMaybeUnknown = lockMaybeUnknown,
        needPriorState = contractIdS,
      )
      val activenessSet = ActivenessSet(
        contracts = contractsCheck,
        reassignmentIds = Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedSynchronizer(
          ReassignmentId(
            parsedRequest.fullViewTree.sourceSynchronizer,
            unassignId(parsedRequest),
          ),
          synchronizerId.unwrap,
        )
      )

  protected override def contractsMaybeUnknown(
      fullView: FullView,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    snapshot.ipsSnapshot
      .hostedOn(fullView.contracts.stakeholders.all, participantId)
      // unassigned contracts may not be known if all the hosted stakeholders are onboarding
      .map(hostedStakeholders =>
        hostedStakeholders.nonEmpty && hostedStakeholders.values.forall(_.onboarding)
      )

  /** Wait until the participant has received and processed all topology transactions on the target
    * synchronizer up to the target-synchronizer time proof timestamp.
    *
    * As we're not processing messages in parallel, delayed message processing on one synchronizer
    * can block message processing on another synchronizer and thus breaks isolation across
    * synchronizers. Even with parallel processing, the cursors in the request journal would not
    * move forward, so event emission to the event log blocks, too.
    *
    * No deadlocks can arise under normal behaviour though. For a deadlock, we would need cyclic
    * waiting, i.e., an unassignment request on one synchronizer D1 references a time proof on
    * another synchronizer D2 and an earlier unassignment request on D2 references a time proof on
    * D3 and so on to synchronizer Dn and an earlier unassignment request on Dn references a later
    * time proof on D1. This, however, violates temporal causality of events.
    *
    * This argument breaks down for malicious participants because the participant cannot verify
    * that the time proof is authentic without having processed all topology updates up to the
    * declared timestamp as the sequencer's signing key might change. So a malicious participant
    * could fake a time proof and set a timestamp in the future, which breaks causality. With
    * unbounded parallel processing of messages, deadlocks cannot occur as this waiting runs in
    * parallel with the request tracker, so time progresses on the target synchronizer and
    * eventually reaches the timestamp.
    */
  // TODO(i12926): Prevent deadlocks. Detect non-sensible timestamps. Verify sequencer signature on time proof.
  private def getTopologySnapshotAtTimestamp(
      synchronizerId: Target[PhysicalSynchronizerId],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Target[TopologySnapshot]] =
    for {
      targetStaticSynchronizerParameters <- reassignmentCoordination
        .getStaticSynchronizerParameter(synchronizerId)

      snapshot <- reassignmentCoordination
        .awaitTimestampAndGetTaggedCryptoSnapshot(
          synchronizerId,
          targetStaticSynchronizerParameters,
          timestamp,
        )
    } yield snapshot.map(_.ipsSnapshot)

  private def unassignId(req: ParsedReassignmentRequest[FullUnassignmentTree]): UnassignId =
    UnassignId(
      req.fullViewTree.sourceSynchronizer,
      req.requestTimestamp,
    )

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      reassignmentLookup: ReassignmentLookup,
      activenessF: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val fullTree: FullUnassignmentTree = parsedRequest.fullViewTree
    val requestCounter = parsedRequest.rc
    val sourceSnapshot = Source(parsedRequest.snapshot.ipsSnapshot)

    val isReassigningParticipant = fullTree.isReassigningParticipant(participantId)
    val unassignmentValidation = new UnassignmentValidation(participantId, contractAuthenticator)
    val reassignmentId = ReassignmentId(synchronizerId.map(_.logical), unassignId(parsedRequest))

    if (isReassigningParticipant) {
      reassignmentCoordination.addPendingUnassignment(reassignmentId)
    }

    for {
      targetTopologyO <-
        if (isReassigningParticipant)
          getTopologySnapshotAtTimestamp(
            fullTree.targetSynchronizer,
            fullTree.targetTimeProof.timestamp,
          ).map(Option(_))
        else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](None)

      unassignmentValidationResult <- unassignmentValidation.perform(
        sourceSnapshot,
        targetTopologyO,
        activenessF,
      )(parsedRequest)

    } yield {
      val responseF =
        createConfirmationResponses(
          parsedRequest.requestId,
          sourceSnapshot.unwrap,
          protocolVersion.unwrap,
          fullTree.confirmingParties,
          unassignmentValidationResult,
        ).map(_.map((_, Recipients.cc(parsedRequest.mediator))))

      // We consider that we rejected if at least one of the responses is not "approve"
      val locallyRejectedF = responseF.map(
        _.exists { case (confirmation, _) =>
          confirmation.responses.exists(response => !response.localVerdict.isApprove)
        }
      )

      val engineAbortStatusF =
        unassignmentValidationResult.contractAuthenticationResultF.value.map {
          case Left(ReassignmentValidationError.ReinterpretationAborted(_, reason)) =>
            EngineAbortStatus.aborted(reason)
          case _ => EngineAbortStatus.notAborted
        }

      val entry = PendingUnassignment(
        parsedRequest.requestId,
        requestCounter,
        parsedRequest.sc,
        unassignmentValidationResult = unassignmentValidationResult,
        parsedRequest.mediator,
        locallyRejectedF,
        engineController.abort,
        engineAbortStatusF = engineAbortStatusF,
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        EitherT.right(responseF),
        RejectionArgs(
          entry,
          LocalRejectError.TimeRejects.LocalTimeout
            .Reject()
            .toLocalReject(protocolVersion.unwrap),
        ),
      )
    }
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: PendingUnassignment,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] = {
    val PendingUnassignment(
      requestId,
      _requestCounter,
      requestSequencerCounter,
      unassignmentValidationResult,
      _mediatorId,
      _locallyRejected,
      _engineController,
      _abortedF,
    ) = pendingRequestData

    val isReassigningParticipant = unassignmentValidationResult.assignmentExclusivity.isDefined
    val pendingSubmissionData = pendingSubmissionMap.get(unassignmentValidationResult.rootHash)
    def rejected(
        reason: TransactionRejection
    ): EitherT[
      FutureUnlessShutdown,
      ReassignmentProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      for {
        eventO <- EitherT.fromEither[FutureUnlessShutdown](
          createRejectionEvent(RejectionArgs(pendingRequestData, reason))
        )
        _ = reassignmentCoordination.completeUnassignment(
          unassignmentValidationResult.reassignmentId
        )
      } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    def mergeRejectionReasons(
        reason: TransactionRejection,
        validationError: Option[TransactionRejection],
    ): TransactionRejection =
      // we reject with the phase 7 rejection, as it is the best information we have
      validationError.getOrElse(reason)

    for {
      rejectionO <- EitherT.right(
        checkPhase7Validations(unassignmentValidationResult)
      )

      commit <- (verdict, rejectionO) match {
        case (_: Verdict.Approve, Some(rejection)) =>
          rejected(rejection)

        case (_: Verdict.Approve, _) =>
          val commitSet = unassignmentValidationResult.commitSet
          val commitSetFO = Some(FutureUnlessShutdown.pure(commitSet))
          val unassignmentData = UnassignmentData(
            reassignmentId = unassignmentValidationResult.reassignmentId,
            unassignmentRequest = unassignmentValidationResult.fullTree,
            unassignmentTs = unassignmentValidationResult.unassignmentTs,
          )
          for {
            _ <- ifThenET(isReassigningParticipant) {
              reassignmentCoordination
                .addUnassignmentRequest(unassignmentData)
                .map { _ =>
                  reassignmentCoordination.completeUnassignment(
                    unassignmentValidationResult.reassignmentId
                  )
                }
            }

            notInitiator = pendingSubmissionData.isEmpty
            _ <-
              if (notInitiator && isReassigningParticipant)
                triggerAssignmentWhenExclusivityTimeoutExceeded(pendingRequestData)
              else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())

            reassignmentAccepted <- EitherT.fromEither[FutureUnlessShutdown](
              unassignmentValidationResult.createReassignmentAccepted(
                participantId,
                requestId.unwrap,
              )
            )
          } yield CommitAndStoreContractsAndPublishEvent(
            commitSetFO,
            Seq.empty,
            Some(reassignmentAccepted),
          )
        case (reasons: Verdict.ParticipantReject, rejectionO) =>
          rejected(mergeRejectionReasons(reasons.keyEvent, rejectionO))

        case (rejection: MediatorReject, rejectionO) =>
          rejected(mergeRejectionReasons(rejection, rejectionO))
      }
    } yield commit

  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    EitherT.rightT(
      reassignmentCoordination.completeUnassignment(
        ReassignmentId(synchronizerId.map(_.logical), unassignId(parsedRequest))
      )
    )

  private[this] def triggerAssignmentWhenExclusivityTimeoutExceeded(
      pendingRequestData: RequestType#PendingRequestData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val targetSynchronizer = pendingRequestData.unassignmentValidationResult.targetSynchronizer
    val t0 = pendingRequestData.unassignmentValidationResult.targetTimeProof.timestamp

    for {
      targetStaticSynchronizerParameters <- reassignmentCoordination
        .getStaticSynchronizerParameter(targetSynchronizer)

      automaticAssignment <- AutomaticAssignment
        .perform(
          pendingRequestData.unassignmentValidationResult.reassignmentId,
          targetSynchronizer,
          targetStaticSynchronizerParameters,
          reassignmentCoordination,
          pendingRequestData.unassignmentValidationResult.stakeholders,
          pendingRequestData.submitterMetadata,
          participantId,
          t0,
        )

    } yield automaticAssignment
  }

  override def localRejectFromActivenessCheck(
      requestId: RequestId,
      validationResult: ReassignmentValidationResult,
  ): Option[LocalRejectError] = {
    import com.digitalasset.canton.ReassignmentCounter
    val activenessResult = validationResult.activenessResult

    def counterIsCorrect(
        contractId: LfContractId,
        declaredReassignmentCounter: ReassignmentCounter,
    ): Boolean = {
      val expectedStatus = Option(ActiveContractStore.Active(declaredReassignmentCounter - 1))
      activenessResult.contracts.priorStates.get(contractId).contains(expectedStatus)
    }

    val incorrectCounter = validationResult.contracts.contractIdCounters.find {
      case (contractId, reassignmentCounter) => !counterIsCorrect(contractId, reassignmentCounter)
    }

    if (incorrectCounter.isDefined)
      incorrectCounter.map { case (contractId, reassignmentCounter) =>
        LocalRejectError.UnassignmentRejects.ActivenessCheckFailed.Reject(
          s"reassignment counter for contract id $contractId is not correct: $reassignmentCounter"
        )
      }
    else if (activenessResult.contracts.notActive.nonEmpty) {
      Some(
        LocalRejectError.ConsistencyRejections.InactiveContracts
          .Reject(activenessResult.contracts.notFree.keys.toSeq.map(_.coid))
      )
    } else if (activenessResult.contracts.alreadyLocked.nonEmpty) {
      Some(
        LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(activenessResult.contracts.alreadyLocked.toSeq.map(_.coid))
      )
    } else if (activenessResult.isSuccessful) None
    else
      throw new RuntimeException(
        s"Unassignment $requestId: Unexpected activeness result $activenessResult"
      )

  }
}

object UnassignmentProcessingSteps {

  final case class SubmissionParam(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractIds: Seq[LfContractId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
  ) {
    val submittingParty: LfPartyId = submitterMetadata.submitter
  }

  final case class SubmissionResult(
      reassignmentId: ReassignmentId,
      unassignmentCompletionF: Future[com.google.rpc.status.Status],
  )

  final case class PendingUnassignment(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      unassignmentValidationResult: UnassignmentValidationResult,
      override val mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends PendingReassignment {

    def isReassigningParticipant: Boolean =
      unassignmentValidationResult.assignmentExclusivity.isDefined

    override def rootHashO: Option[RootHash] = Some(unassignmentValidationResult.rootHash)

    override def submitterMetadata: ReassignmentSubmitterMetadata =
      unassignmentValidationResult.submitterMetadata
  }
}
