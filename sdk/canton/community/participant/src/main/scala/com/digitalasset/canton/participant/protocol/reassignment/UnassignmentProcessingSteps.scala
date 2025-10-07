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
  SynchronizerCryptoClient,
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
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingSteps}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  Purged,
  ReassignedAway,
}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{ContractValidator, MonadUtil}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class UnassignmentProcessingSteps(
    val synchronizerId: Source[PhysicalSynchronizerId],
    val participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    sourceCrypto: SynchronizerCryptoClient,
    seedGenerator: SeedGenerator,
    staticSynchronizerParameters: Source[StaticSynchronizerParameters],
    override protected val contractValidator: ContractValidator,
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

  override type RequestType = ProcessingSteps.RequestType.Unassignment
  override val requestType: RequestType = ProcessingSteps.RequestType.Unassignment

  override def reassignmentId(
      fullViewTree: FullUnassignmentTree,
      requestTimestamp: CantonTimestamp,
  ): ReassignmentId = ReassignmentId(
    fullViewTree.sourceSynchronizer.map(_.logical),
    fullViewTree.targetSynchronizer.map(_.logical),
    requestTimestamp,
    fullViewTree.contracts.contractIdCounters,
  )

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
      ephemeralState: SyncEphemeralState,
      sourceRecentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (Submission, PendingSubmissionData),
  ] = {
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
        TargetSynchronizerIsSourceSynchronizer(synchronizerId.unwrap, contractIds),
      )

      targetStaticSynchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentCoordination
          .getStaticSynchronizerParameter(targetSynchronizer)
      )
      targetTopology <- reassignmentCoordination
        .getRecentTopologySnapshot(
          targetSynchronizer,
          targetStaticSynchronizerParameters,
        )
      targetTimestamp = targetTopology.map(_.timestamp)
      _ = logger.debug(withDetails(s"Picked target timestamp $targetTimestamp"))

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
          contracts,
          submitterMetadata,
          synchronizerId,
          mediator,
          targetSynchronizer,
          Source(sourceRecentSnapshot.ipsSnapshot),
          targetTopology,
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
      pendingSubmission <-
        performPendingSubmissionMapUpdate(
          pendingSubmissions(ephemeralState),
          ReassignmentRef(submissionParam.contractIds.toSet),
          submissionParam.submittingParty,
          rootHash,
          validated.request.mkReassignmentId,
        )
    } yield (
      ReassignmentsSubmission(Batch.of(protocolVersion.unwrap, messages*), rootHash),
      Some(pendingSubmission),
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: PendingSubmissionData,
  ): SubmissionResult =
    SubmissionResult(
      pendingSubmission.value.mkReassignmentId(deliver.timestamp),
      pendingSubmission.value.reassignmentCompletion.future,
    )

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
        UnexpectedSynchronizer(parsedRequest.reassignmentId, synchronizerId.unwrap)
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

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      reassignmentLookup: ReassignmentLookup,
      activenessF: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val fullTree: FullUnassignmentTree = parsedRequest.fullViewTree
    val requestCounter = parsedRequest.rc

    val isReassigningParticipant = fullTree.isReassigningParticipant(participantId)
    if (isReassigningParticipant) {
      reassignmentCoordination.addPendingUnassignment(
        parsedRequest.reassignmentId,
        fullTree.sourceSynchronizer.map(_.logical),
      )
    }

    val unassignmentValidation = UnassignmentValidation(
      isReassigningParticipant,
      participantId,
      contractValidator,
      activenessF,
      reassignmentCoordination,
    )

    for {
      unassignmentValidationResult <- unassignmentValidation.perform(parsedRequest)
    } yield {
      val confirmationResponseF =
        if (
          unassignmentValidationResult.reassigningParticipantValidationResult.isTargetTsValidatable
        ) {
          createConfirmationResponses(
            parsedRequest.requestId,
            parsedRequest.malformedPayloads,
            protocolVersion.unwrap,
            unassignmentValidationResult,
          )
        } else {
          logger.info(
            s"Sending an abstain verdict for ${unassignmentValidationResult.hostedConfirmingReassigningParties} because target timestamp is not validatable"
          )
          FutureUnlessShutdown.pure(
            createAbstainResponse(
              parsedRequest.requestId,
              unassignmentValidationResult.rootHash,
              s"Non-validatable target timestamp when processing unassignment ${parsedRequest.reassignmentId}",
              unassignmentValidationResult.hostedConfirmingReassigningParties,
            )
          )
        }
      val responseF =
        confirmationResponseF.map(_.map((_, Recipients.cc(parsedRequest.mediator))))

      // We consider that we rejected if at least one of the responses is a "reject"
      val locallyRejectedF = responseF.map(
        _.exists { case (confirmation, _) =>
          confirmation.responses.exists(_.localVerdict.isReject)
        }
      )

      val engineAbortStatusF =
        unassignmentValidationResult.commonValidationResult.contractAuthenticationResultF.value
          .map {
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
        decisionTimeTickRequest,
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        EitherT.right(responseF),
        RejectionArgs(
          entry,
          ErrorDetails.fromLocalError(
            LocalRejectError.TimeRejects.LocalTimeout
              .Reject()
          ),
        ),
      )
    }
  }

  override def getCommitSetAndContractsToBeStoredAndEventFactory(
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
      _decisionTimeTickRequest,
    ) = pendingRequestData

    val isReassigningParticipant = unassignmentValidationResult.assignmentExclusivity.isDefined
    val pendingSubmissionData = pendingSubmissionMap.get(unassignmentValidationResult.rootHash)
    def rejected(
        errorDetails: ErrorDetails
    ): EitherT[
      FutureUnlessShutdown,
      ReassignmentProcessorError,
      CommitAndStoreContractsAndPublishEvent,
    ] =
      for {
        eventO <- EitherT.fromEither[FutureUnlessShutdown](
          createRejectionEvent(RejectionArgs(pendingRequestData, errorDetails))
        )
        _ = reassignmentCoordination.completeUnassignment(
          unassignmentValidationResult.reassignmentId,
          unassignmentValidationResult.sourceSynchronizer,
        )
      } yield CommitAndStoreContractsAndPublishEvent(
        None,
        Seq.empty,
        eventO.map(event => _ => _ => event),
      )

    def mergeRejectionReasons(
        validationError: Option[LocalRejectError],
        errorDetails: ErrorDetails,
    ): ErrorDetails =
      // we reject with the phase 7 rejection, as it is the best information we have
      validationError
        .map(e => ErrorDetails(e.reason(), e.isMalformed))
        .getOrElse(errorDetails)

    for {
      rejectionFromPhase3 <- EitherT.right(
        checkPhase7Validations(unassignmentValidationResult)
      )

      // Additional validation requested during security audit as DIA-003-013.
      // Activeness of the mediator already gets checked in Phase 3,
      // this additional validation covers the case that the mediator gets deactivated between Phase 3 and Phase 7.
      resultTs = event.event.content.timestamp
      topologySnapshotAtTs <- EitherT(
        sourceCrypto.ips.awaitSnapshot(resultTs).map(snapshot => Either.right(Source(snapshot)))
      )

      mediatorCheckResultO <- EitherT.right(
        ReassignmentValidation
          .ensureMediatorActive(
            topologySnapshotAtTs,
            mediator = pendingRequestData.mediator,
            reassignmentId = unassignmentValidationResult.reassignmentId,
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

      commit <- (verdict, rejectionO) match {
        case (_: Verdict.Approve, Some(rejection)) =>
          rejected(ErrorDetails.fromLocalError(rejection))

        case (_: Verdict.Approve, _) =>
          val commitSet = unassignmentValidationResult.commitSet
          val commitSetFO = Some(FutureUnlessShutdown.pure(commitSet))
          val unassignmentData = unassignmentValidationResult.unassignmentData
          for {
            _ <- ifThenET(isReassigningParticipant) {
              reassignmentCoordination
                .addUnassignmentRequest(unassignmentData)
                .map { _ =>
                  reassignmentCoordination.completeUnassignment(
                    unassignmentValidationResult.reassignmentId,
                    unassignmentValidationResult.sourceSynchronizer,
                  )
                }
            }

            notInitiator = pendingSubmissionData.isEmpty
            _ <-
              if (notInitiator && isReassigningParticipant)
                triggerAssignmentWhenExclusivityTimeoutExceeded(pendingRequestData)
              else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())

            reassignmentAccepted =
              unassignmentValidationResult.createReassignmentAccepted(
                participantId,
                requestId.unwrap,
              )
          } yield CommitAndStoreContractsAndPublishEvent(
            commitSetFO,
            Seq.empty,
            Some(reassignmentAccepted),
          )
        case (reasons: Verdict.ParticipantReject, rejectionO) =>
          val errorDetails = mergeRejectionReasons(rejectionO, reasons.keyErrorDetails)
          rejected(errorDetails)

        case (rejection: MediatorReject, rejectionO) =>
          val errorDetails =
            mergeRejectionReasons(rejectionO, rejection.errorDetails)
          rejected(errorDetails)
      }
    } yield commit

  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    EitherT.rightT(
      reassignmentCoordination.completeUnassignment(
        parsedRequest.reassignmentId,
        parsedRequest.fullViewTree.sourceSynchronizer,
      )
    )

  private[this] def triggerAssignmentWhenExclusivityTimeoutExceeded(
      pendingRequestData: RequestType#PendingRequestData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val targetSynchronizer = pendingRequestData.unassignmentValidationResult.targetSynchronizer
    val t0 = pendingRequestData.unassignmentValidationResult.targetTimestamp

    for {
      targetStaticSynchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentCoordination
          .getStaticSynchronizerParameter(targetSynchronizer)
      )

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
    val activenessResult = validationResult.commonValidationResult.activenessResult

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
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  ) extends PendingReassignment {

    def isReassigningParticipant: Boolean =
      unassignmentValidationResult.assignmentExclusivity.isDefined

    override def rootHashO: Option[RootHash] = Some(unassignmentValidationResult.rootHash)

    override def submitterMetadata: ReassignmentSubmitterMetadata =
      unassignmentValidationResult.submitterMetadata

    override def cancelDecisionTimeTickRequest(): Unit = decisionTimeTickRequest.cancel()
  }
}
