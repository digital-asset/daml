// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DecryptionError as _, EncryptionError as _, *}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingSteps}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{ContractValidator, EitherTUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class AssignmentProcessingSteps(
    val psid: Target[PhysicalSynchronizerId],
    val participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    targetCrypto: SynchronizerCryptoClient,
    seedGenerator: SeedGenerator,
    override protected val contractValidator: ContractValidator,
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    clock: Clock,
    val protocolVersion: Target[ProtocolVersion],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ReassignmentProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      AssignmentViewType,
      PendingAssignment,
    ]
    with NamedLogging {

  import AssignmentProcessingSteps.*

  override def requestKind: String = "Assignment"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submitterMetadata.submitter}, reassignmentId ${param.reassignmentId}"

  override def explicitMediatorGroup(param: SubmissionParam): Option[MediatorGroupIndex] = None

  override type RequestType = ProcessingSteps.RequestType.Assignment
  override val requestType = ProcessingSteps.RequestType.Assignment

  override def reassignmentId(
      fullViewTree: FullAssignmentTree,
      requestTimestamp: CantonTimestamp,
  ): ReassignmentId = fullViewTree.reassignmentId

  override def pendingSubmissions(state: SyncEphemeralState): PendingSubmissions =
    state.pendingAssignmentSubmissions

  private val assignmentValidation = new AssignmentValidation(
    psid,
    staticSynchronizerParameters,
    participantId,
    reassignmentCoordination,
    contractValidator,
    loggerFactory,
  )

  override def submissionIdOfPendingRequest(pendingData: PendingAssignment): RootHash =
    pendingData.assignmentValidationResult.rootHash

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralState,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (Submission, PendingSubmissionData),
  ] = {
    val approximateTimestampOverride = Some(clock.now)

    val SubmissionParam(
      submitterMetadata,
      reassignmentId,
    ) = submissionParam
    val topologySnapshot = Target(recentSnapshot.ipsSnapshot)
    val pureCrypto = recentSnapshot.pureCrypto
    val submitter = submitterMetadata.submitter

    def activeParticipantsOfParty(
        parties: Seq[LfPartyId]
    ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Set[ParticipantId]] = EitherT(
      topologySnapshot.unwrap.activeParticipantsOfParties(parties).map {
        partyToParticipantAttributes =>
          partyToParticipantAttributes.toSeq
            .traverse { case (party, participants) =>
              Either.cond(
                participants.nonEmpty,
                participants,
                NoParticipantForReceivingParty(reassignmentId, party): ReassignmentProcessorError,
              )
            }
            .map(_.toSet.flatten)
      }
    )

    for {
      unassignmentData <- ephemeralState.reassignmentLookup
        .lookup(reassignmentId)
        .leftMap(err => NoReassignmentData(reassignmentId, err))

      sourceSynchronizer = unassignmentData.sourcePSId

      /*
       Because an upgrade of the target synchronizer can happen between unassignment
       and assignment, the comparison needs to be logical.
       */
      _ = if (unassignmentData.targetPSId.map(_.logical) != psid.map(_.logical))
        throw new IllegalStateException(
          s"Assignment $reassignmentId: Reassignment data for ${unassignmentData.targetPSId
              .map(_.logical)} found on wrong synchronizer ${psid.map(_.logical)}"
        )

      stakeholders = unassignmentData.stakeholders
      _ <- ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef(reassignmentId),
          topologySnapshot,
          submitter,
          participantId,
          stakeholders = stakeholders.all,
        )
        .leftMap(_.toSubmissionValidationError)

      assignmentUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()

      fullTree <- EitherT.fromEither[FutureUnlessShutdown](
        makeFullAssignmentTree(
          pureCrypto,
          seed,
          reassignmentId,
          submitterMetadata,
          unassignmentData.contractsBatch,
          sourceSynchronizer,
          psid,
          mediator,
          assignmentUuid,
          protocolVersion,
          unassignmentData.reassigningParticipants,
          unassignmentData.unassignmentTs,
        )
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- recentSnapshot
        .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly, approximateTimestampOverride)
        .leftMap(ReassignmentSigningError.apply)
      mediatorMessage = fullTree.mediatorMessage(
        submittingParticipantSignature,
        staticSynchronizerParameters.map(_.protocolVersion),
      )
      recipientsSet <- activeParticipantsOfParty(stakeholders.all.toSeq)
      contractIds = unassignmentData.contractsBatch.contractIds.toSeq
      recipients <- EitherT.fromEither[FutureUnlessShutdown](
        Recipients
          .ofSet(recipientsSet)
          .toRight(NoStakeholders.logAndCreate(contractIds, logger))
      )
      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          Seq(
            (ViewHashAndRecipients(fullTree.viewHash, recipients), None, fullTree.informees.toList)
          ),
          parallel = true,
          pureCrypto,
          recentSnapshot,
          ephemeralState.sessionKeyStoreLookup.convertStore,
        )
        .leftMap[ReassignmentProcessorError](
          EncryptionError(contractIds, _)
        )
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
      viewMessage <- EncryptedViewMessageFactory
        .create(AssignmentViewType)(
          fullTree,
          (viewKey, viewKeyMap),
          recentSnapshot,
          approximateTimestampOverride,
          protocolVersion.unwrap,
        )
        .leftMap[ReassignmentProcessorError](
          EncryptionError(contractIds, _)
        )
      rootHashMessage =
        RootHashMessage(
          rootHash,
          psid.unwrap,
          ViewType.AssignmentViewType,
          recentSnapshot.ipsSnapshot.timestamp,
          EmptyRootHashMessagePayload,
        )
      // Each member gets a message sent to itself and to the mediator
      rootHashRecipients =
        Recipients.recipientGroups(
          checked(
            NonEmptyUtil.fromUnsafe(
              recipientsSet.toSeq.map(participant =>
                NonEmpty(Set, mediator, MemberRecipient(participant))
              )
            )
          )
        )
      messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediator),
        viewMessage -> recipients,
        rootHashMessage -> rootHashRecipients,
      )
      pendingSubmission <-
        performPendingSubmissionMapUpdate(
          pendingSubmissions(ephemeralState),
          ReassignmentRef(submissionParam.reassignmentId),
          submissionParam.submitterLf,
          rootHash,
          _ => reassignmentId,
        )
    } yield (
      ReassignmentsSubmission(Batch.of(protocolVersion.unwrap, messages*), rootHash),
      Some(pendingSubmission),
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[?]],
      pendingSubmission: PendingSubmissionData,
  ): SubmissionResult =
    SubmissionResult(pendingSubmission.value.reassignmentCompletion.future)

  override protected def decryptTree(
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[AssignmentViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageError,
    (WithRecipients[FullAssignmentTree], Option[Signature]),
  ] =
    EncryptedViewMessage
      .decryptFor(
        snapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullAssignmentTree
          .fromByteString(snapshot.pureCrypto, protocolVersion)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(fullTree =>
        (
          WithRecipients(fullTree, envelope.recipients),
          envelope.protocolMessage.submittingParticipantSignature,
        )
      )

  override def computeActivenessSet(
      parsedRequest: ParsedReassignmentRequest[FullAssignmentTree]
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, ActivenessSet] =
    if (Target(parsedRequest.fullViewTree.psid) == psid) {
      val contractIds = parsedRequest.fullViewTree.contracts.contractIds.toSet
      val contractCheck = ActivenessCheck.tryCreate(
        checkFresh = Set.empty,
        checkFree = contractIds,
        checkActive = Set.empty,
        lock = contractIds,
        lockMaybeUnknown = Set.empty,
        needPriorState = Set.empty,
      )
      val activenessSet = ActivenessSet(
        contracts = contractCheck,
        reassignmentIds =
          if (parsedRequest.fullViewTree.isReassigningParticipant(participantId))
            Set(parsedRequest.reassignmentId)
          else Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedSynchronizer(
          parsedRequest.reassignmentId,
          targetSynchronizerId = parsedRequest.fullViewTree.psid,
          receivedOn = psid.unwrap,
        )
      )

  // assigned contracts should always be "known" as assignments include the contracts
  protected override def contractsMaybeUnknown(
      fullView: FullView,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    FutureUnlessShutdown.pure(false)

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedRequestType,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val reassignmentId = parsedRequest.reassignmentId
    val sourceSynchronizer = parsedRequest.fullViewTree.sourceSynchronizer
    val isReassigningParticipant =
      parsedRequest.fullViewTree.isReassigningParticipant(participantId)

    for {
      reassignmentDataE <- EitherT.right[ReassignmentProcessorError](
        reassignmentCoordination
          .waitForStartedUnassignmentToCompletePhase7(reassignmentId, sourceSynchronizer)
          .flatMap(_ => reassignmentLookup.lookup(reassignmentId).value)
      )

      assignmentValidationResult <- assignmentValidation
        .perform(
          reassignmentDataE,
          activenessResultFuture,
        )(parsedRequest)

    } yield {
      val confirmationResponseF =
        if (assignmentValidationResult.hostedConfirmingReassigningParties.isEmpty) {
          logger.debug(
            "Not sending a verdict because the list of hosted confirming parties is empty"
          )
          FutureUnlessShutdown.pure(None)
        } else if (
          assignmentValidationResult.reassigningParticipantValidationResult.isUnassignmentDataNotFound && isReassigningParticipant
        ) {
          logger.info(
            s"Sending an abstain verdict for ${assignmentValidationResult.hostedConfirmingReassigningParties} because unassignment data is not found in the reassignment store"
          )
          val confirmationResponses = createAbstainResponse(
            parsedRequest.requestId,
            assignmentValidationResult.rootHash,
            s"Unassignment data not found when processing assignment $reassignmentId.",
            assignmentValidationResult.hostedConfirmingReassigningParties,
          )

          FutureUnlessShutdown.pure(confirmationResponses)
        } else {
          createConfirmationResponses(
            parsedRequest.requestId,
            parsedRequest.malformedPayloads,
            protocolVersion.unwrap,
            assignmentValidationResult,
          )
        }

      val responseF = confirmationResponseF.map(_.map((_, Recipients.cc(parsedRequest.mediator))))

      // We consider that we rejected if we fail to process or if at least one of the responses is a "reject"
      val locallyRejectedF = responseF.map(
        _.exists { case (confirmation, _) =>
          confirmation.responses.exists(_.localVerdict.isReject)
        }
      )

      val engineAbortStatusF =
        assignmentValidationResult.commonValidationResult.contractAuthenticationResultF.value.map {
          case Left(ReassignmentValidationError.ReinterpretationAborted(_, reason)) =>
            EngineAbortStatus.aborted(reason)
          case _ => EngineAbortStatus.notAborted
        }

      // construct pending data and response
      val entry = PendingAssignment(
        parsedRequest.requestId,
        parsedRequest.rc,
        parsedRequest.sc,
        assignmentValidationResult,
        parsedRequest.mediator,
        locallyRejectedF,
        engineController.abort,
        engineAbortStatusF,
        decisionTimeTickRequest,
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        EitherT.right(responseF),
        RejectionArgs(
          entry,
          ErrorDetails.fromLocalError(
            LocalRejectError.TimeRejects.LocalTimeout.Reject()
          ),
        ),
      )
    }
  }

  override def getCommitSetAndContractsToBeStoredAndEventFactory(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: PendingAssignment,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] = {
    val PendingAssignment(
      requestId,
      _requestCounter,
      _requestSequencerCounter,
      assignmentValidationResult,
      _,
      _locallyRejectedF,
      _engineController,
      _abortedF,
      _decisionTimeTickRequest,
    ) = pendingRequestData

    def rejected(
        errorDetails: ErrorDetails
    ): EitherT[
      FutureUnlessShutdown,
      ReassignmentProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] = {
      val commit = for {
        eventO <- createRejectionEvent(RejectionArgs(pendingRequestData, errorDetails))
      } yield CommitAndStoreContractsAndPublishEvent(
        None,
        Seq.empty,
        eventO.map(event => _ => _ => event),
      )
      EitherT.fromEither[FutureUnlessShutdown](commit)
    }

    def mergeRejectionReasons(
        validationError: Option[LocalRejectError],
        errorDetails: ErrorDetails,
    ): ErrorDetails =
      // we reject with the phase 7 rejection, as it is the best information we have
      validationError
        .map(e => ErrorDetails(e.reason(), e.isMalformed))
        .getOrElse(errorDetails)

    for {
      rejectionFromPhase3 <- EitherT.right(checkPhase7Validations(assignmentValidationResult))

      // Additional validation requested during security audit as DIA-003-013.
      // Activeness of the mediator already gets checked in Phase 3,
      // this additional validation covers the case that the mediator gets deactivated between Phase 3 and Phase 7.
      resultTs = event.event.content.timestamp
      topologySnapshotAtTs <- EitherT(
        targetCrypto.ips.awaitSnapshot(resultTs).map(snapshot => Either.right(Target(snapshot)))
      )

      mediatorCheckResultO <- EitherT.right(
        ReassignmentValidation
          .ensureMediatorActive(
            topologySnapshotAtTs,
            mediator = pendingRequestData.mediator,
            reassignmentId = assignmentValidationResult.reassignmentId,
          )
          .value
          .map(
            _.swap.toOption.map(error =>
              LocalRejectError.MalformedRejects.MalformedRequest
                .Reject(s"${error.message}. Rolling back.")
            )
          )
      )

      rejectionO = mediatorCheckResultO.orElse(rejectionFromPhase3)

      commitAndStoreContract <- (verdict, rejectionO) match {
        case (_: Verdict.Approve, Some(rejection)) =>
          rejected(ErrorDetails.fromLocalError(rejection))
        case (_: Verdict.Approve, _) =>
          val commitSet = assignmentValidationResult.commitSet
          val commitSetO = Some(FutureUnlessShutdown.pure(commitSet))
          val contractsToBeStored = assignmentValidationResult.contracts.contracts.map(_.contract)

          for {
            _ <-
              if (
                assignmentValidationResult.reassigningParticipantValidationResult.isUnassignmentDataNotFound
                && assignmentValidationResult.isReassigningParticipant
              ) {
                reassignmentCoordination.addAssignmentData(
                  assignmentValidationResult.reassignmentId,
                  contracts = assignmentValidationResult.contracts,
                  source = assignmentValidationResult.sourcePSId.map(_.logical),
                  target = psid.map(_.logical),
                )
              } else EitherTUtil.unitUS[ReassignmentProcessorError]
            update = assignmentValidationResult.createReassignmentAccepted(
              psid.map(_.logical),
              participantId,
              requestId.unwrap,
            )
          } yield CommitAndStoreContractsAndPublishEvent(
            commitSetO,
            contractsToBeStored,
            Some(update),
          )

        case (reasons: Verdict.ParticipantReject, rejectionO) =>
          val mergedError = mergeRejectionReasons(rejectionO, reasons.keyErrorDetails)
          rejected(mergedError)

        case (rejection: Verdict.MediatorReject, rejectionO) =>
          val mergedError =
            mergeRejectionReasons(rejectionO, rejection.errorDetails)
          rejected(mergedError)
      }
    } yield commitAndStoreContract
  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())

  override def localRejectFromActivenessCheck(
      requestId: RequestId,
      validationResult: ReassignmentValidationResult,
  ): Option[LocalRejectError] = {
    val activenessResult = validationResult.commonValidationResult.activenessResult
    if (validationResult.activenessResultIsSuccessful) None
    else if (activenessResult.inactiveReassignments.contains(validationResult.reassignmentId))
      Some(
        LocalRejectError.AssignmentRejects.AlreadyCompleted
          .Reject("")
      )
    else if (activenessResult.contracts.notFree.nonEmpty)
      Some(
        LocalRejectError.AssignmentRejects.ActivatesExistingContracts
          .Reject(activenessResult.contracts.notFree.keys.toSeq.map(_.coid))
      )
    else if (activenessResult.contracts.alreadyLocked.nonEmpty)
      Some(
        LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(activenessResult.contracts.alreadyLocked.toSeq.map(_.coid))
      )
    else
      throw new RuntimeException(
        s"Assignment $requestId: Unexpected activeness result $activenessResult"
      )
  }
}

object AssignmentProcessingSteps {

  final case class SubmissionParam(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  ) {
    val submitterLf: LfPartyId = submitterMetadata.submitter
  }

  final case class SubmissionResult(assignmentCompletionF: Future[com.google.rpc.status.Status])

  final case class PendingAssignment(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      assignmentValidationResult: AssignmentValidationResult,
      mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  ) extends PendingReassignment {

    override def rootHashO: Option[RootHash] = Some(assignmentValidationResult.rootHash)

    override def submitterMetadata: ReassignmentSubmitterMetadata =
      assignmentValidationResult.submitterMetadata

    override def cancelDecisionTimeTickRequest(): Unit = decisionTimeTickRequest.cancel()
  }

  private[reassignment] def makeFullAssignmentTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      reassignmentId: ReassignmentId,
      submitterMetadata: ReassignmentSubmitterMetadata,
      contracts: ContractsReassignmentBatch,
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
      targetMediator: MediatorGroupRecipient,
      assignmentUuid: UUID,
      targetProtocolVersion: Target[ProtocolVersion],
      reassigningParticipants: Set[ParticipantId],
      unassignmentTs: CantonTimestamp,
  ): Either[ReassignmentProcessorError, FullAssignmentTree] = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)
    val stakeholders = contracts.stakeholders

    val commonData = AssignmentCommonData
      .create(pureCrypto)(
        commonDataSalt,
        sourceSynchronizer,
        targetSynchronizer,
        targetMediator,
        stakeholders = stakeholders,
        uuid = assignmentUuid,
        submitterMetadata,
        reassigningParticipants,
        unassignmentTs,
      )

    for {
      view <- AssignmentView
        .create(pureCrypto)(
          viewSalt,
          reassignmentId,
          contracts,
          targetProtocolVersion,
        )
        .leftMap(reason => InvalidReassignmentView(reason))
      tree = AssignmentViewTree(commonData, view, targetProtocolVersion, pureCrypto)
    } yield FullAssignmentTree(tree)
  }
}
