// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.participant.protocol.{
  EngineController,
  ProcessingSteps,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
  checked,
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class AssignmentProcessingSteps(
    val synchronizerId: Target[SynchronizerId],
    val participantId: ParticipantId,
    val engine: DAMLe,
    reassignmentCoordination: ReassignmentCoordination,
    seedGenerator: SeedGenerator,
    override protected val serializableContractAuthenticator: SerializableContractAuthenticator,
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    targetProtocolVersion: Target[ProtocolVersion],
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

  override type SubmissionResultArgs = PendingReassignmentSubmission

  override type RequestType = ProcessingSteps.RequestType.Assignment
  override val requestType = ProcessingSteps.RequestType.Assignment

  override def pendingSubmissions(state: SyncEphemeralState): PendingSubmissions =
    state.pendingAssignmentSubmissions

  private val assignmentValidation = new AssignmentValidation(
    synchronizerId,
    staticSynchronizerParameters,
    participantId,
    reassignmentCoordination,
    engine,
    loggerFactory,
  )

  override def submissionIdOfPendingRequest(pendingData: PendingAssignment): RootHash =
    pendingData.assignmentValidationResult.rootHash

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralStateLookup,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Submission] = {

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
      reassignmentData <- ephemeralState.reassignmentLookup
        .lookup(reassignmentId)
        .leftMap(err => NoReassignmentData(reassignmentId, err))

      unassignmentResult <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentData.unassignmentResult.toRight(
          UnassignmentIncomplete(reassignmentId, participantId)
        )
      )

      targetSynchronizer = reassignmentData.targetSynchronizer
      _ = if (targetSynchronizer != synchronizerId)
        throw new IllegalStateException(
          s"Assignment $reassignmentId: Reassignment data for ${reassignmentData.targetSynchronizer} found on wrong synchronizer $synchronizerId"
        )

      stakeholders = reassignmentData.unassignmentRequest.stakeholders
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
          submitterMetadata,
          reassignmentData.contract,
          reassignmentData.reassignmentCounter,
          targetSynchronizer,
          mediator,
          unassignmentResult,
          assignmentUuid,
          reassignmentData.sourceProtocolVersion,
          targetProtocolVersion,
          reassignmentData.unassignmentRequest.reassigningParticipants,
        )
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- recentSnapshot
        .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
        .leftMap(ReassignmentSigningError.apply)
      mediatorMessage = fullTree.mediatorMessage(submittingParticipantSignature)
      recipientsSet <- activeParticipantsOfParty(stakeholders.all.toSeq)
      recipients <- EitherT.fromEither[FutureUnlessShutdown](
        Recipients
          .ofSet(recipientsSet)
          .toRight(NoStakeholders.logAndCreate(reassignmentData.contract.contractId, logger))
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
          EncryptionError(reassignmentData.contract.contractId, _)
        )
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
      viewMessage <- EncryptedViewMessageFactory
        .create(AssignmentViewType)(
          fullTree,
          (viewKey, viewKeyMap),
          recentSnapshot,
          targetProtocolVersion.unwrap,
        )
        .leftMap[ReassignmentProcessorError](
          EncryptionError(reassignmentData.contract.contractId, _)
        )
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          synchronizerId.unwrap,
          targetProtocolVersion.unwrap,
          ViewType.AssignmentViewType,
          recentSnapshot.ipsSnapshot.timestamp,
          EmptyRootHashMessagePayload,
        )
      // Each member gets a message sent to itself and to the mediator
      val rootHashRecipients =
        Recipients.recipientGroups(
          checked(
            NonEmptyUtil.fromUnsafe(
              recipientsSet.toSeq.map(participant =>
                NonEmpty(Set, mediator, MemberRecipient(participant))
              )
            )
          )
        )
      val messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediator),
        viewMessage -> recipients,
        rootHashMessage -> rootHashRecipients,
      )
      ReassignmentsSubmission(Batch.of(targetProtocolVersion.unwrap, messages*), rootHash)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      submissionId: PendingSubmissionId,
  ): EitherT[Future, ReassignmentProcessorError, SubmissionResultArgs] =
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      ReassignmentRef(submissionParam.reassignmentId),
      submissionParam.submitterLf,
      submissionId,
    )

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult =
    SubmissionResult(pendingSubmission.reassignmentCompletion.future)

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
        staticSynchronizerParameters.unwrap,
        snapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullAssignmentTree
          .fromByteString(snapshot.pureCrypto, targetProtocolVersion)(bytes)
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
    // TODO(i12926): Send a rejection if malformedPayloads is non-empty
    if (parsedRequest.fullViewTree.targetSynchronizer == synchronizerId) {
      val contractId = parsedRequest.fullViewTree.contract.contractId
      val contractCheck = ActivenessCheck.tryCreate(
        checkFresh = Set.empty,
        checkFree = Set(contractId),
        checkActive = Set.empty,
        lock = Set(contractId),
        needPriorState = Set.empty,
      )
      val activenessSet = ActivenessSet(
        contracts = contractCheck,
        reassignmentIds =
          if (parsedRequest.fullViewTree.isReassigningParticipant(participantId))
            Set(parsedRequest.fullViewTree.unassignmentResultEvent.reassignmentId)
          else Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedDomain(
          parsedRequest.fullViewTree.unassignmentResultEvent.reassignmentId,
          targetSynchronizerId = parsedRequest.fullViewTree.synchronizerId,
          receivedOn = synchronizerId.unwrap,
        )
      )

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedRequestType,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val reassignmentId = parsedRequest.fullViewTree.unassignmentResultEvent.reassignmentId
    for {
      reassignmentDataE <- EitherT
        .right[ReassignmentProcessorError](
          reassignmentLookup.lookup(reassignmentId).value
        )

      assignmentValidationResult <- assignmentValidation
        .perform(
          Target(parsedRequest.snapshot),
          reassignmentDataE,
          activenessResultFuture,
          engineController,
        )(parsedRequest)

    } yield {
      val responseF = createConfirmationResponse(
        parsedRequest.requestId,
        parsedRequest.snapshot.ipsSnapshot,
        targetProtocolVersion.unwrap,
        parsedRequest.fullViewTree.confirmingParties,
        assignmentValidationResult,
      ).map(_.map((_, Recipients.cc(parsedRequest.mediator))).toList)

      // We consider that we rejected if we fail to process or if at least one of the responses is not "approve'
      val locallyRejectedF = responseF.map(
        _.exists { case (confirmation, _) => !confirmation.localVerdict.isApprove }
      )

      val engineAbortStatusF = assignmentValidationResult.metadataResultET.value.map {
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
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        EitherT.right(responseF),
        RejectionArgs(
          entry,
          LocalRejectError.TimeRejects.LocalTimeout
            .Reject()
            .toLocalReject(targetProtocolVersion.unwrap),
        ),
      )
    }
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
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
      requestCounter,
      requestSequencerCounter,
      assignmentValidationResult,
      _,
      _locallyRejectedF,
      _engineController,
      _abortedF,
    ) = pendingRequestData

    def rejected(
        reason: TransactionRejection
    ): Either[ReassignmentProcessorError, CommitAndStoreContractsAndPublishEvent] =
      for {
        eventO <- createRejectionEvent(RejectionArgs(pendingRequestData, reason))
      } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    for {
      isSuccessful <- EitherT.right(assignmentValidationResult.isSuccessfulF)
      commitAndStoreContractE = verdict match {
        // TODO(i22887): Right now we fail at phase 7 if any validation has failed.
        // We should fail only for some specific errors e.g ModelConformance check.
        case _: Verdict.Approve if !isSuccessful =>
          throw new RuntimeException(
            s"Assignment validation failed for $requestId because: ${assignmentValidationResult.validationResult}"
          )

        case _: Verdict.Approve =>
          val commitSet = assignmentValidationResult.commitSet
          val commitSetO = Some(FutureUnlessShutdown.pure(commitSet))
          val contractsToBeStored = Seq(assignmentValidationResult.contract)

          for {
            update <- assignmentValidationResult.createReassignmentAccepted(
              synchronizerId,
              participantId,
              targetProtocolVersion,
              requestId.unwrap,
              requestCounter,
              requestSequencerCounter,
            )
          } yield CommitAndStoreContractsAndPublishEvent(
            commitSetO,
            contractsToBeStored,
            Some(update),
          )

        case reasons: Verdict.ParticipantReject => rejected(reasons.keyEvent)

        case rejection: Verdict.MediatorReject => rejected(rejection)
      }
      commitAndStoreContract <- EitherT.fromEither[FutureUnlessShutdown](commitAndStoreContractE)
    } yield commitAndStoreContract

  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())

  override def localRejectFromActivenessCheck(
      requestId: RequestId,
      activenessResult: ActivenessResult,
      validationResult: ReassignmentValidationResult,
  ): Option[LocalRejectError] =
    if (activenessResult.inactiveReassignments.nonEmpty)
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
    else if (activenessResult.isSuccessful) None
    else
      throw new RuntimeException(
        s"Assignment $requestId: Unexpected activeness result $activenessResult"
      )

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
  ) extends PendingReassignment {

    override def rootHashO: Option[RootHash] = Some(assignmentValidationResult.rootHash)

    override def submitterMetadata: ReassignmentSubmitterMetadata =
      assignmentValidationResult.submitterMetadata
  }

  private[reassignment] def makeFullAssignmentTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      submitterMetadata: ReassignmentSubmitterMetadata,
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
      targetSynchronizer: Target[SynchronizerId],
      targetMediator: MediatorGroupRecipient,
      unassignmentResult: DeliveredUnassignmentResult,
      assignmentUuid: UUID,
      sourceProtocolVersion: Source[ProtocolVersion],
      targetProtocolVersion: Target[ProtocolVersion],
      reassigningParticipants: Set[ParticipantId],
  ): Either[ReassignmentProcessorError, FullAssignmentTree] = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)
    val stakeholders = Stakeholders(contract.metadata)

    val commonData = AssignmentCommonData
      .create(pureCrypto)(
        commonDataSalt,
        targetSynchronizer,
        targetMediator,
        stakeholders = stakeholders,
        uuid = assignmentUuid,
        submitterMetadata,
        targetProtocolVersion,
        reassigningParticipants,
      )

    for {
      view <- AssignmentView
        .create(pureCrypto)(
          viewSalt,
          contract,
          unassignmentResult,
          sourceProtocolVersion,
          targetProtocolVersion,
          reassignmentCounter,
        )
        .leftMap(reason => InvalidReassignmentView(reason))
      tree = AssignmentViewTree(commonData, view, targetProtocolVersion, pureCrypto)
    } yield FullAssignmentTree(tree)
  }
}
