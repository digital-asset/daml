// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  DomainIndex,
  Reassignment,
  ReassignmentInfo,
  RequestIndex,
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
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.ReassignmentSigningError
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  TargetDomainIsSourceDomain,
  UnexpectedDomain,
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
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPackageName,
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
  checked,
}

import scala.concurrent.{ExecutionContext, Future}

class UnassignmentProcessingSteps(
    val domainId: Source[DomainId],
    val participantId: ParticipantId,
    val engine: DAMLe,
    reassignmentCoordination: ReassignmentCoordination,
    seedGenerator: SeedGenerator,
    staticDomainParameters: Source[StaticDomainParameters],
    val sourceDomainProtocolVersion: Source[ProtocolVersion],
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

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions =
    state.pendingUnassignmentSubmissions

  override def requestKind: String = "Unassignment"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submittingParty}, contract ${param.contractId}, target ${param.targetDomain}"

  override def submissionIdOfPendingRequest(pendingData: PendingUnassignment): RootHash =
    pendingData.rootHash

  private def targetIsNotSource(contractId: LfContractId, target: Target[DomainId])(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      target.unwrap != domainId.unwrap,
      TargetDomainIsSourceDomain(domainId.unwrap, contractId),
    )

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncDomainEphemeralStateLookup,
      sourceRecentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Submission] = {
    val SubmissionParam(
      submitterMetadata,
      contractId,
      targetDomain,
      targetProtocolVersion,
    ) = submissionParam
    val pureCrypto = sourceRecentSnapshot.pureCrypto

    def withDetails(message: String) = s"unassign $contractId to $targetDomain: $message"

    for {
      _ <- targetIsNotSource(contractId, targetDomain)
      storedContract <- getStoredContract(ephemeralState.contractLookup, contractId)

      targetStaticDomainParameters <- reassignmentCoordination
        .getStaticDomainParameter(targetDomain)
        .mapK(FutureUnlessShutdown.outcomeK)

      timeProofAndSnapshot <- reassignmentCoordination.getTimeProofAndSnapshot(
        targetDomain,
        targetStaticDomainParameters,
      )
      (timeProof, targetCrypto) = timeProofAndSnapshot
      _ = logger.debug(withDetails(s"Picked time proof ${timeProof.timestamp}"))

      reassignmentCounter <- EitherT(
        ephemeralState.tracker
          .getApproximateStates(Seq(contractId))
          .map(_.get(contractId) match {
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
          })
      ).mapK(FutureUnlessShutdown.outcomeK)

      newReassignmentCounter <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentCounter.increment
          .leftMap(_ => UnassignmentProcessorError.ReassignmentCounterOverflow)
      )

      creatingTransactionId <- EitherT.fromEither[FutureUnlessShutdown](
        storedContract.creatingTransactionIdO.toRight(CreatingTransactionIdNotFound(contractId))
      )

      validated <- UnassignmentRequest.validated(
        participantId,
        timeProof,
        creatingTransactionId,
        storedContract.contract,
        submitterMetadata,
        domainId,
        sourceDomainProtocolVersion,
        mediator,
        targetDomain,
        targetProtocolVersion,
        Source(sourceRecentSnapshot.ipsSnapshot),
        targetCrypto.map(_.ipsSnapshot),
        newReassignmentCounter,
      )

      unassignmentUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()
      fullTree = validated.request.toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        unassignmentUuid,
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- sourceRecentSnapshot
        .sign(rootHash.unwrap)
        .leftMap(ReassignmentSigningError.apply)
      mediatorMessage = fullTree.mediatorMessage(submittingParticipantSignature)
      maybeRecipients = Recipients.ofSet(validated.recipients)
      recipientsT <- EitherT
        .fromOption[FutureUnlessShutdown](
          maybeRecipients,
          NoStakeholders.logAndCreate(contractId, logger): ReassignmentProcessorError,
        )
      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          Seq(
            (ViewHashAndRecipients(fullTree.viewHash, recipientsT), None, fullTree.informees.toList)
          ),
          parallel = true,
          pureCrypto,
          sourceRecentSnapshot,
          ephemeralState.sessionKeyStoreLookup.convertStore,
          targetProtocolVersion.unwrap,
        )
        .leftMap[ReassignmentProcessorError](EncryptionError(contractId, _))
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
      viewMessage <- EncryptedViewMessageFactory
        .create(UnassignmentViewType)(
          fullTree,
          (viewKey, viewKeyMap),
          sourceRecentSnapshot,
          sourceDomainProtocolVersion.unwrap,
        )
        .leftMap[ReassignmentProcessorError](EncryptionError(contractId, _))
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId.unwrap,
          sourceDomainProtocolVersion.unwrap,
          ViewType.UnassignmentViewType,
          sourceRecentSnapshot.ipsSnapshot.timestamp,
          EmptyRootHashMessagePayload,
        )
      val rootHashRecipients =
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
      val messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediator),
        viewMessage -> recipientsT,
        rootHashMessage -> rootHashRecipients,
      )
      ReassignmentsSubmission(Batch.of(sourceDomainProtocolVersion.unwrap, messages*), rootHash)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, ReassignmentProcessorError, SubmissionResultArgs] =
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      None,
      submissionParam.submittingParty,
      pendingSubmissionId,
    )

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult = {
    val requestId = RequestId(deliver.timestamp)
    val reassignmentId = ReassignmentId(domainId, requestId.unwrap)
    SubmissionResult(reassignmentId, pendingSubmission.reassignmentCompletion.future)
  }

  private[this] def getStoredContract(
      contractLookup: ContractLookup,
      contractId: LfContractId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, StoredContract] =
    contractLookup
      .lookup(contractId)
      .toRight[ReassignmentProcessorError](UnassignmentProcessorError.UnknownContract(contractId))
      .mapK(FutureUnlessShutdown.outcomeK)

  override protected def decryptTree(
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[UnassignmentViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, WithRecipients[
    FullUnassignmentTree
  ]] =
    EncryptedViewMessage
      .decryptFor(
        staticDomainParameters.unwrap,
        sourceSnapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullUnassignmentTree
          .fromByteString(sourceSnapshot.pureCrypto, sourceDomainProtocolVersion)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(WithRecipients(_, envelope.recipients))

  override def computeActivenessSet(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, ActivenessSet] =
    // TODO(i12926): Send a rejection if malformedPayloads is non-empty
    if (parsedRequest.fullViewTree.sourceDomain == domainId) {
      val contractId = parsedRequest.fullViewTree.contractId
      val contractIdS = Set(contractId)
      val contractsCheck = ActivenessCheck.tryCreate(
        checkFresh = Set.empty,
        checkFree = Set.empty,
        checkActive = contractIdS,
        lock = contractIdS,
        needPriorState = contractIdS,
      )
      val activenessSet = ActivenessSet(
        contracts = contractsCheck,
        reassignmentIds = Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedDomain(
          ReassignmentId(parsedRequest.fullViewTree.sourceDomain, parsedRequest.requestTimestamp),
          domainId.unwrap,
        )
      )

  /** Wait until the participant has received and processed all topology transactions on the target domain
    * up to the target-domain time proof timestamp.
    *
    * As we're not processing messages in parallel, delayed message processing on one domain can
    * block message processing on another domain and thus breaks isolation across domains.
    * Even with parallel processing, the cursors in the request journal would not move forward,
    * so event emission to the event log blocks, too.
    *
    * No deadlocks can arise under normal behaviour though.
    * For a deadlock, we would need cyclic waiting, i.e., an unassignment request on one domain D1 references
    * a time proof on another domain D2 and an earlier unassignment request on D2 references a time proof on D3
    * and so on to domain Dn and an earlier unassignment request on Dn references a later time proof on D1.
    * This, however, violates temporal causality of events.
    *
    * This argument breaks down for malicious participants
    * because the participant cannot verify that the time proof is authentic without having processed
    * all topology updates up to the declared timestamp as the sequencer's signing key might change.
    * So a malicious participant could fake a time proof and set a timestamp in the future,
    * which breaks causality.
    * With unbounded parallel processing of messages, deadlocks cannot occur as this waiting runs in parallel with
    * the request tracker, so time progresses on the target domain and eventually reaches the timestamp.
    */
  // TODO(i12926): Prevent deadlocks. Detect non-sensible timestamps. Verify sequencer signature on time proof.
  private def getTopologySnapshotAtTimestamp(
      domainId: Target[DomainId],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Target[TopologySnapshot]] =
    for {
      targetStaticDomainParameters <- reassignmentCoordination
        .getStaticDomainParameter(domainId)
        .mapK(FutureUnlessShutdown.outcomeK)
      snapshot <- reassignmentCoordination
        .awaitTimestampAndGetTaggedCryptoSnapshot(
          domainId,
          targetStaticDomainParameters,
          timestamp,
        )
    } yield snapshot.map(_.ipsSnapshot)

  override def constructPendingDataAndResponse(
      parsedRequestType: ParsedReassignmentRequest[FullUnassignmentTree],
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
    val ParsedReassignmentRequest(
      rc,
      ts,
      sc,
      fullTree,
      recipients,
      _,
      _,
      _,
      _,
      _,
      mediator,
      sourceSnapshot,
      _,
    ) =
      parsedRequestType

    val reassignmentId: ReassignmentId = ReassignmentId(fullTree.sourceDomain, ts)
    val view = fullTree.tree.view.tryUnwrap
    val contract = view.contract
    val isReassigningParticipant = fullTree.isReassigningParticipant(participantId)

    for {
      // Since the unassignment request should be sent only to participants that host a stakeholder of the contract,
      // we can expect to find the contract in the contract store.

      // TODO(i15090): Validate contract data against contract id and contract metadata against contract data

      targetTopology <-
        if (isReassigningParticipant)
          getTopologySnapshotAtTimestamp(
            fullTree.targetDomain,
            fullTree.targetTimeProof.timestamp,
          ).map(Some(_))
        else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](None)

      _ <- UnassignmentValidation.perform(
        expectedStakeholders = Stakeholders(contract.metadata),
        contract.rawContractInstance.contractInstance.unversioned.template,
        sourceDomainProtocolVersion,
        Source(sourceSnapshot.ipsSnapshot),
        targetTopology,
        recipients,
      )(fullTree)

      assignmentExclusivity <- getAssignmentExclusivity(
        targetTopology,
        fullTree.targetTimeProof.timestamp,
        fullTree.targetDomain,
      )

      activenessResult <- EitherT.right(activenessF)

      hostedStks <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          hostedStakeholders(fullTree.stakeholders.toList, sourceSnapshot.ipsSnapshot)
        )
      )

      requestId = RequestId(ts)
      unassignmentDecisionTime <- ProcessingSteps
        .getDecisionTime(sourceSnapshot.ipsSnapshot, ts)
        .leftMap(ReassignmentParametersError(domainId.unwrap, _))
        .mapK(FutureUnlessShutdown.outcomeK)

      reassignmentData = ReassignmentData(
        sourceProtocolVersion = sourceDomainProtocolVersion,
        unassignmentTs = ts,
        unassignmentRequestCounter = rc,
        unassignmentRequest = fullTree,
        unassignmentDecisionTime = unassignmentDecisionTime,
        contract = contract,
        unassignmentResult = None,
        reassignmentGlobalOffset = None,
      )
      _ <- ifThenET(isReassigningParticipant) {
        reassignmentCoordination.addUnassignmentRequest(reassignmentData)
      }
      confirmingStakeholders <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          sourceSnapshot.ipsSnapshot.canConfirm(
            participantId,
            contract.metadata.stakeholders,
          )
        )
      )
      responseOpt = createUnassignmentResponse(
        requestId,
        isReassigningParticipant,
        activenessResult,
        contract.contractId,
        fullTree.reassignmentCounter,
        confirmingStakeholders,
        fullTree.tree.rootHash,
      )
    } yield {
      // We consider that we rejected if at least one of the responses is not "approve'
      val locallyRejectedF = FutureUnlessShutdown.pure(responseOpt.exists { response =>
        !response.localVerdict.isApprove
      })

      val entry = PendingUnassignment(
        requestId,
        rc,
        sc,
        fullTree.tree.rootHash,
        fullTree.contractId,
        fullTree.reassignmentCounter,
        contract.rawContractInstance.contractInstance.unversioned.template,
        contract.rawContractInstance.contractInstance.unversioned.packageName,
        isReassigningParticipant,
        fullTree.submitterMetadata,
        reassignmentId,
        fullTree.targetDomain,
        fullTree.stakeholders,
        hostedStks.toSet,
        fullTree.targetTimeProof,
        assignmentExclusivity,
        mediator,
        locallyRejectedF,
        engineController.abort,
        engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        EitherT.pure[FutureUnlessShutdown, RequestError](
          responseOpt.map(_ -> Recipients.cc(mediator)).toList
        ),
        RejectionArgs(
          entry,
          LocalRejectError.TimeRejects.LocalTimeout
            .Reject()
            .toLocalReject(sourceDomainProtocolVersion.unwrap),
        ),
      )
    }
  }

  private[this] def getAssignmentExclusivity(
      targetTopology: Option[Target[TopologySnapshot]],
      timestamp: CantonTimestamp,
      domainId: Target[DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentParametersError, Option[Target[CantonTimestamp]]] =
    targetTopology.traverse { targetTopology =>
      ProcessingSteps
        .getAssignmentExclusivity(targetTopology, timestamp)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap(ReassignmentParametersError(domainId.unwrap, _))
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
      requestCounter,
      requestSequencerCounter,
      rootHash,
      contractId,
      reassignmentCounter,
      templateId,
      packageName,
      isReassigningParticipant,
      submitterMetadata,
      reassignmentId,
      targetDomain,
      stakeholders,
      hostedStakeholders,
      _targetTimeProof,
      assignmentExclusivity,
      _mediatorId,
      _locallyRejected,
      _engineController,
      _abortedF,
    ) = pendingRequestData

    val pendingSubmissionData = pendingSubmissionMap.get(rootHash)

    def rejected(
        reason: TransactionRejection
    ): EitherT[Future, ReassignmentProcessorError, CommitAndStoreContractsAndPublishEvent] = for {
      _ <- ifThenET(isReassigningParticipant)(deleteReassignment(targetDomain, requestId))

      eventO <- EitherT.fromEither[Future](
        createRejectionEvent(RejectionArgs(pendingRequestData, reason))
      )
    } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    verdict match {
      case _: Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          unassignments = Map(
            contractId -> CommitSet
              .UnassignmentCommit(targetDomain, stakeholders, reassignmentCounter)
          ),
          assignments = Map.empty,
        )
        val commitSetFO = Some(Future.successful(commitSet))
        for {
          _ <- ifThenET(isReassigningParticipant) {
            EitherT
              .fromEither[FutureUnlessShutdown](DeliveredUnassignmentResult.create(event))
              .leftMap(err => UnassignmentProcessorError.InvalidResult(reassignmentId, err))
              .flatMap(deliveredResult =>
                reassignmentCoordination.addUnassignmentResult(targetDomain, deliveredResult)
              )
          }

          notInitiator = pendingSubmissionData.isEmpty
          _ <-
            if (notInitiator && isReassigningParticipant)
              triggerAssignmentWhenExclusivityTimeoutExceeded(pendingRequestData)
            else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())

          reassignmentAccepted <- createReassignmentAccepted(
            contractId,
            templateId,
            packageName,
            stakeholders,
            submitterMetadata,
            reassignmentId,
            targetDomain,
            rootHash,
            assignmentExclusivity,
            isReassigningParticipant = isReassigningParticipant,
            reassignmentCounter,
            hostedStakeholders.toList,
            requestCounter,
            requestSequencerCounter,
          ).mapK(FutureUnlessShutdown.outcomeK)
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetFO,
          Seq.empty,
          Some(Traced(reassignmentAccepted)),
        )

      case reasons: Verdict.ParticipantReject =>
        rejected(reasons.keyEvent).mapK(FutureUnlessShutdown.outcomeK)

      case rejection: MediatorReject => rejected(rejection).mapK(FutureUnlessShutdown.outcomeK)
    }
  }

  override def handleTimeout(parsedRequest: ParsedReassignmentRequest[FullView])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    deleteReassignment(parsedRequest.fullViewTree.targetDomain, parsedRequest.requestId)
      .mapK(FutureUnlessShutdown.outcomeK)

  private def createReassignmentAccepted(
      contractId: LfContractId,
      templateId: LfTemplateId,
      packageName: LfPackageName,
      contractStakeholders: Set[LfPartyId],
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
      targetDomain: Target[DomainId],
      rootHash: RootHash,
      assignmentExclusivity: Option[Target[CantonTimestamp]],
      isReassigningParticipant: Boolean,
      reassignmentCounter: ReassignmentCounter,
      hostedStakeholders: List[LfPartyId],
      requestCounter: RequestCounter,
      requestSequencerCounter: SequencerCounter,
  ): EitherT[Future, ReassignmentProcessorError, Update.ReassignmentAccepted] =
    for {
      updateId <- EitherT
        .fromEither[Future](rootHash.asLedgerTransactionId)
        .leftMap[ReassignmentProcessorError](
          FieldConversionError(reassignmentId, "Transaction Id", _)
        )

      completionInfo =
        Option.when(participantId == submitterMetadata.submittingParticipant)(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            applicationId = submitterMetadata.applicationId,
            commandId = submitterMetadata.commandId,
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
            messageUuid = None,
          )
        )
    } yield Update.ReassignmentAccepted(
      optCompletionInfo = completionInfo,
      workflowId = submitterMetadata.workflowId,
      updateId = updateId,
      recordTime = reassignmentId.unassignmentTs.underlying,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = reassignmentId.sourceDomain,
        targetDomain = targetDomain,
        submitter = Option(submitterMetadata.submitter),
        reassignmentCounter = reassignmentCounter.unwrap,
        hostedStakeholders = hostedStakeholders,
        unassignId = reassignmentId.unassignmentTs,
        isReassigningParticipant = isReassigningParticipant,
      ),
      reassignment = Reassignment.Unassign(
        contractId = contractId,
        templateId = templateId,
        packageName = packageName,
        stakeholders = contractStakeholders.toList,
        assignmentExclusivity = assignmentExclusivity.map(_.unwrap.toLf),
      ),
      domainIndex = Some(
        DomainIndex.of(
          RequestIndex(
            counter = requestCounter,
            sequencerCounter = Some(requestSequencerCounter),
            timestamp = reassignmentId.unassignmentTs,
          )
        )
      ),
    )

  private[this] def triggerAssignmentWhenExclusivityTimeoutExceeded(
      pendingRequestData: RequestType#PendingRequestData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val targetDomain = pendingRequestData.targetDomain
    val t0 = pendingRequestData.targetTimeProof.timestamp

    (for {
      targetStaticDomainParameters <- reassignmentCoordination
        .getStaticDomainParameter(targetDomain)

      automaticAssignment <- AutomaticAssignment
        .perform(
          pendingRequestData.reassignmentId,
          targetDomain,
          targetStaticDomainParameters,
          reassignmentCoordination,
          pendingRequestData.stakeholders,
          pendingRequestData.submitterMetadata,
          participantId,
          t0,
        )

    } yield automaticAssignment).mapK(FutureUnlessShutdown.outcomeK)
  }

  private[this] def deleteReassignment(
      targetDomain: Target[DomainId],
      unassignmentRequestId: RequestId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] = {
    val reassignmentId = ReassignmentId(domainId, unassignmentRequestId.unwrap)
    reassignmentCoordination.deleteReassignment(targetDomain, reassignmentId)
  }

  private[this] def createUnassignmentResponse(
      requestId: RequestId,
      isReassigningParticipant: Boolean,
      activenessResult: ActivenessResult,
      contractId: LfContractId,
      declaredReassignmentCounter: ReassignmentCounter,
      confirmingStakeholders: Set[LfPartyId],
      rootHash: RootHash,
  ): Option[ConfirmationResponse] = {
    val expectedPriorReassignmentCounter = Map[LfContractId, Option[ActiveContractStore.Status]](
      contractId -> Some(ActiveContractStore.Active(declaredReassignmentCounter - 1))
    )

    val successful =
      declaredReassignmentCounter > ReassignmentCounter.Genesis &&
        activenessResult.isSuccessful &&
        activenessResult.contracts.priorStates == expectedPriorReassignmentCounter

    // send a response only if the participant is a reassigning participant or the activeness check has failed
    if (isReassigningParticipant || !successful) {
      val localVerdict =
        if (successful) LocalApprove(sourceDomainProtocolVersion.unwrap)
        else
          LocalRejectError.UnassignmentRejects.ActivenessCheckFailed
            .Reject(s"$activenessResult")
            .toLocalReject(sourceDomainProtocolVersion.unwrap)
      val response = checked(
        ConfirmationResponse.tryCreate(
          requestId,
          participantId,
          Some(ViewPosition.root),
          localVerdict,
          rootHash,
          confirmingStakeholders,
          domainId.unwrap,
          sourceDomainProtocolVersion.unwrap,
        )
      )
      Some(response)
    } else None
  }
}

object UnassignmentProcessingSteps {

  final case class SubmissionParam(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: Target[DomainId],
      targetProtocolVersion: Target[ProtocolVersion],
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
      rootHash: RootHash,
      contractId: LfContractId,
      reassignmentCounter: ReassignmentCounter,
      templateId: LfTemplateId,
      packageName: LfPackageName,
      isReassigningParticipant: Boolean,
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
      targetDomain: Target[DomainId],
      stakeholders: Set[LfPartyId],
      hostedStakeholders: Set[LfPartyId],
      targetTimeProof: TimeProof,
      assignmentExclusivity: Option[Target[CantonTimestamp]],
      mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends PendingReassignment {

    override def rootHashO: Option[RootHash] = Some(rootHash)
  }
}
