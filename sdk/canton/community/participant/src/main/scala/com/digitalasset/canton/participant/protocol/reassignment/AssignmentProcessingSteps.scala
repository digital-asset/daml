// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  Reassignment,
  ReassignmentInfo,
  SequencedUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
  CommitSet,
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
import com.digitalasset.canton.participant.protocol.validation.{
  AuthenticationError,
  AuthenticationValidator,
}
import com.digitalasset.canton.participant.protocol.{
  EngineController,
  ProcessingSteps,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.Archived
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
  checked,
}
import com.digitalasset.daml.lf.data.Bytes

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class AssignmentProcessingSteps(
    val domainId: Target[DomainId],
    val participantId: ParticipantId,
    val engine: DAMLe,
    reassignmentCoordination: ReassignmentCoordination,
    seedGenerator: SeedGenerator,
    serializableContractAuthenticator: SerializableContractAuthenticator,
    staticDomainParameters: Target[StaticDomainParameters],
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

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions =
    state.pendingAssignmentSubmissions

  private val assignmentValidation = new AssignmentValidation(
    domainId,
    serializableContractAuthenticator,
    staticDomainParameters,
    participantId,
    reassignmentCoordination,
    loggerFactory,
  )

  private val reassignmentValidation = new ReassignmentValidation(engine, loggerFactory)

  override def submissionIdOfPendingRequest(pendingData: PendingAssignment): RootHash =
    pendingData.rootHash

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
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
    ): EitherT[Future, ReassignmentProcessorError, Set[ParticipantId]] = EitherT(
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

      targetDomain = reassignmentData.targetDomain
      _ = if (targetDomain != domainId)
        throw new IllegalStateException(
          s"Assignment $reassignmentId: Reassignment data for ${reassignmentData.targetDomain} found on wrong domain $domainId"
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
        .mapK(FutureUnlessShutdown.outcomeK)

      assignmentUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()

      fullTree <- EitherT.fromEither[FutureUnlessShutdown](
        makeFullAssignmentTree(
          pureCrypto,
          seed,
          submitterMetadata,
          reassignmentData.contract,
          reassignmentData.reassignmentCounter,
          targetDomain,
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
        .sign(rootHash.unwrap)
        .leftMap(ReassignmentSigningError.apply)
      mediatorMessage = fullTree.mediatorMessage(submittingParticipantSignature)
      recipientsSet <- activeParticipantsOfParty(stakeholders.all.toSeq).mapK(
        FutureUnlessShutdown.outcomeK
      )
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
          targetProtocolVersion.unwrap,
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
          domainId.unwrap,
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
      snapshot: DomainSnapshotSyncCryptoApi,
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
        staticDomainParameters.unwrap,
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
    if (parsedRequest.fullViewTree.targetDomain == domainId) {
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
          if (parsedRequest.isReassigningParticipant)
            Set(parsedRequest.fullViewTree.unassignmentResultEvent.reassignmentId)
          else Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedDomain(
          parsedRequest.fullViewTree.unassignmentResultEvent.reassignmentId,
          targetDomain = parsedRequest.fullViewTree.domainId,
          receivedOn = domainId.unwrap,
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

    val ParsedReassignmentRequest(
      rc,
      ts,
      sc,
      fullViewTree,
      _,
      _,
      _,
      _,
      isReassigningParticipant,
      _,
      mediator,
      targetCrypto,
      _,
    ) = parsedRequest

    val requestId = RequestId(ts)
    val reassignmentId = fullViewTree.unassignmentResultEvent.reassignmentId

    // We perform the stakeholders check asynchronously so that we can complete the pending request
    // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
    // mediator verdict.
    val stakeholdersCheckResultET = reassignmentValidation
      .checkMetadata(
        fullViewTree,
        getEngineAbortStatus = () => engineController.abortStatus,
      )
      .mapK(FutureUnlessShutdown.outcomeK)

    for {
      hostedStakeholders <- EitherT.right[ReassignmentProcessorError](
        FutureUnlessShutdown.outcomeF(
          targetCrypto.ipsSnapshot
            .hostedOn(fullViewTree.contract.metadata.stakeholders, participantId)
            .map(_.keySet)
        )
      )

      reassignmentDataO <- EitherT
        .right[ReassignmentProcessorError](
          reassignmentLookup.lookup(reassignmentId).toOption.value
        )

      authenticationErrorO <- EitherT
        .right(AuthenticationValidator.verifyViewSignature(parsedRequest))

      confirmingSignatories <- EitherTUtil.rightUS(
        targetCrypto.ipsSnapshot.canConfirm(
          participantId,
          fullViewTree.contract.metadata.signatories,
        )
      )

      isSignatoryAssigning = confirmingSignatories.nonEmpty &&
        fullViewTree.isReassigningParticipant(participantId)

      validationResultO <- assignmentValidation
        .validateAssignmentRequest(
          ts,
          fullViewTree,
          reassignmentDataO,
          Target(targetCrypto),
          isSignatoryAssigning = isSignatoryAssigning,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      activenessResult <- EitherT.right[ReassignmentProcessorError](activenessResultFuture)
    } yield {
      val responseET: EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Seq[
        (ConfirmationResponse, Recipients)
      ]] =
        stakeholdersCheckResultET.map { _ =>
          createConfirmationResponse(
            requestId,
            fullViewTree,
            activenessResult,
            validationResultO,
            authenticationErrorO,
            Recipients.cc(mediator),
          ).toList
        }

      // We consider that we rejected if we fail to process or if at least one of the responses is not "approve'
      val locallyRejectedF = responseET.value.map(
        _.fold(
          _ => true,
          _.exists { case (confirmation, _) => !confirmation.localVerdict.isApprove },
        )
      )

      val engineAbortStatusF = stakeholdersCheckResultET.value.map {
        case Left(ReinterpretationAborted(_, reason)) => EngineAbortStatus.aborted(reason)
        case _ => EngineAbortStatus.notAborted
      }

      // construct pending data and response
      val entry = PendingAssignment(
        requestId,
        rc,
        sc,
        fullViewTree.rootHash,
        fullViewTree.contract,
        fullViewTree.reassignmentCounter,
        fullViewTree.submitterMetadata,
        isReassigningParticipant = isReassigningParticipant,
        reassignmentId,
        hostedStakeholders,
        mediator,
        locallyRejectedF,
        engineController.abort,
        engineAbortStatusF,
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        responseET,
        RejectionArgs(
          entry,
          LocalRejectError.TimeRejects.LocalTimeout
            .Reject()
            .toLocalReject(targetProtocolVersion.unwrap),
        ),
      )
    }
  }

  // TODO(i22372): Share code with UnassignmentProcessingSteps
  private def createConfirmationResponse(
      requestId: RequestId,
      assignmentRequest: FullAssignmentTree,
      activenessResult: ActivenessResult,
      validationResultO: Option[AssignmentValidationResult],
      authenticationErrorO: Option[AuthenticationError],
      recipients: Recipients,
  )(implicit
      traceContext: TraceContext
  ): Option[(ConfirmationResponse, Recipients)] =
    validationResultO match {
      case None => None

      case Some(validationResult) =>
        val contractResult = activenessResult.contracts

        val localRejectErrorO =
          if (activenessResult.isSuccessful)
            None
          else if (contractResult.notFree.nonEmpty) {
            contractResult.notFree.toSeq match {
              case Seq((coid, Archived)) =>
                Some(
                  LocalRejectError.AssignmentRejects.ContractAlreadyArchived
                    .Reject(show"coid=$coid")
                )
              case Seq((coid, _state)) =>
                Some(
                  LocalRejectError.AssignmentRejects.ContractAlreadyActive
                    .Reject(show"coid=$coid")
                )
              case coids =>
                throw new RuntimeException(
                  s"Activeness result for an assignment fails for multiple contract IDs $coids"
                )
            }
          } else if (contractResult.alreadyLocked.nonEmpty)
            Some(
              LocalRejectError.AssignmentRejects.ContractIsLocked
                .Reject("")
            )
          else if (activenessResult.inactiveReassignments.nonEmpty)
            Some(
              LocalRejectError.AssignmentRejects.AlreadyCompleted
                .Reject("")
            )
          else
            throw new RuntimeException(
              withRequestId(requestId, s"Unexpected activeness result $activenessResult")
            )

        val authenticationRejection = authenticationErrorO.map(err =>
          LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.message)
        )

        val localRejections = (localRejectErrorO.toList ++ authenticationRejection.toList).map {
          err =>
            err.logWithContext()
            err.toLocalReject(targetProtocolVersion.unwrap)
        }

        val localVerdictAndPartiesO = localRejections
          .collectFirst[(LocalVerdict, Set[LfPartyId])] {
            case malformed: LocalReject if malformed.isMalformed => malformed -> Set.empty
            case localReject: LocalReject if validationResult.confirmingParties.nonEmpty =>
              localReject -> validationResult.confirmingParties
          }
          .orElse(
            Option.when(validationResult.confirmingParties.nonEmpty)(
              LocalApprove(targetProtocolVersion.unwrap) -> validationResult.confirmingParties
            )
          )
        val confirmationResponse = localVerdictAndPartiesO.map { case (localVerdict, parties) =>
          checked(
            ConfirmationResponse
              .tryCreate(
                requestId = requestId,
                sender = participantId,
                viewPositionO = Some(ViewPosition.root),
                localVerdict = localVerdict,
                rootHash = assignmentRequest.rootHash,
                confirmingParties = parties,
                domainId = domainId.unwrap,
                protocolVersion = targetProtocolVersion.unwrap,
              )
          )
        }
        confirmationResponse.map(_ -> recipients)
    }

  private[this] def withRequestId(requestId: RequestId, message: String) =
    s"Assignment $requestId: $message"

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
      rootHash,
      contract,
      reassignmentCounter,
      submitterMetadata,
      isReassigningParticipant,
      reassignmentId,
      hostedStakeholders,
      _,
      _locallyRejectedF,
      _engineController,
      _abortedF,
    ) = pendingRequestData

    def rejected(
        reason: TransactionRejection
    ): EitherT[Future, ReassignmentProcessorError, CommitAndStoreContractsAndPublishEvent] = for {
      eventO <- EitherT.fromEither[Future](
        createRejectionEvent(RejectionArgs(pendingRequestData, reason))
      )
    } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    verdict match {
      case _: Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          unassignments = Map.empty,
          assignments = Map(
            contract.contractId ->
              CommitSet.AssignmentCommit(
                reassignmentId,
                contract.metadata,
                reassignmentCounter,
              )
          ),
        )
        val commitSetO = Some(Future.successful(commitSet))
        val contractsToBeStored = Seq(contract)

        for {
          update <- createReassignmentAccepted(
            contract,
            requestId.unwrap,
            submitterMetadata,
            reassignmentId,
            rootHash,
            isReassigningParticipant = isReassigningParticipant,
            reassignmentCounter,
            hostedStakeholders.toList,
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
  }.mapK(FutureUnlessShutdown.outcomeK)

  private[reassignment] def createReassignmentAccepted(
      contract: SerializableContract,
      recordTime: CantonTimestamp,
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
      rootHash: RootHash,
      isReassigningParticipant: Boolean,
      reassignmentCounter: ReassignmentCounter,
      hostedStakeholders: List[LfPartyId],
      requestCounter: RequestCounter,
      requestSequencerCounter: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, SequencedUpdate] = {
    val targetDomain = domainId
    val contractInst = contract.contractInstance.unversioned
    val createNode: LfNodeCreate =
      LfNodeCreate(
        coid = contract.contractId,
        templateId = contractInst.template,
        packageName = contractInst.packageName,
        arg = contractInst.arg,
        signatories = contract.metadata.signatories,
        stakeholders = contract.metadata.stakeholders,
        keyOpt = contract.metadata.maybeKeyWithMaintainers,
        version = contract.contractInstance.version,
      )
    val driverContractMetadata = contract.contractSalt
      .map { salt =>
        DriverContractMetadata(salt).toLfBytes(targetProtocolVersion.unwrap)
      }
      .getOrElse(Bytes.Empty)

    for {
      updateId <- EitherT.fromEither[Future](
        rootHash.asLedgerTransactionId.leftMap[ReassignmentProcessorError](
          FieldConversionError(reassignmentId, "Transaction id (root hash)", _)
        )
      )
      completionInfo =
        Option.when(participantId == submitterMetadata.submittingParticipant)(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            applicationId = submitterMetadata.applicationId,
            commandId = submitterMetadata.commandId,
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
          )
        )
    } yield Update.SequencedReassignmentAccepted(
      optCompletionInfo = completionInfo,
      workflowId = submitterMetadata.workflowId,
      updateId = updateId,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = reassignmentId.sourceDomain,
        targetDomain = targetDomain,
        submitter = Option(submitterMetadata.submitter),
        reassignmentCounter = reassignmentCounter.unwrap,
        hostedStakeholders = hostedStakeholders,
        unassignId = reassignmentId.unassignmentTs,
        isReassigningParticipant = isReassigningParticipant,
      ),
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = contract.ledgerCreateTime.toLf,
        createNode = createNode,
        contractMetadata = driverContractMetadata,
      ),
      requestCounter = requestCounter,
      sequencerCounter = requestSequencerCounter,
      recordTime = recordTime,
    )
  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())
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
      rootHash: RootHash,
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
      submitterMetadata: ReassignmentSubmitterMetadata,
      isReassigningParticipant: Boolean,
      reassignmentId: ReassignmentId,
      hostedStakeholders: Set[LfPartyId],
      mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends PendingReassignment {

    override def rootHashO: Option[RootHash] = Some(rootHash)
  }

  private[reassignment] def makeFullAssignmentTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      submitterMetadata: ReassignmentSubmitterMetadata,
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
      targetDomain: Target[DomainId],
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
        targetDomain,
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
