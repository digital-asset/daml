// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps, Signature}
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.TransferSigningError
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.{
  TargetDomainIsSourceDomain,
  UnexpectedDomain,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, ProtocolProcessor}
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  Active,
  Archived,
  TransferredAway,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  LfPackageName,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  TransferCounter,
  checked,
}

import scala.concurrent.{ExecutionContext, Future}

class TransferOutProcessingSteps(
    val domainId: SourceDomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    val sourceDomainProtocolVersion: SourceProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferOutViewType,
      PendingTransferOut,
    ]
    with NamedLogging {

  override type SubmissionResultArgs = PendingTransferSubmission

  override type PendingDataAndResponseArgs = TransferOutProcessingSteps.PendingDataAndResponseArgs

  override type RequestType = ProcessingSteps.RequestType.TransferOut
  override val requestType: RequestType = ProcessingSteps.RequestType.TransferOut

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions = {
    state.pendingTransferOutSubmissions
  }

  override def requestKind: String = "TransferOut"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submittingParty}, contract ${param.contractId}, target ${param.targetDomain}"

  override def submissionIdOfPendingRequest(pendingData: PendingTransferOut): RootHash =
    pendingData.rootHash

  private def targetIsNotSource(contractId: LfContractId, target: TargetDomainId)(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      target.unwrap != domainId.unwrap,
      TargetDomainIsSourceDomain(domainId.unwrap, contractId),
    )

  override def prepareSubmission(
      param: SubmissionParam,
      mediator: MediatorsOfDomain,
      ephemeralState: SyncDomainEphemeralStateLookup,
      sourceRecentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Submission] = {
    val SubmissionParam(
      submitterMetadata,
      contractId,
      targetDomain,
      targetProtocolVersion,
    ) = param
    val pureCrypto = sourceRecentSnapshot.pureCrypto

    def withDetails(message: String) = s"Transfer-out $contractId to $targetDomain: $message"

    for {
      _ <- targetIsNotSource(contractId, targetDomain)
      storedContract <- getStoredContract(ephemeralState.contractLookup, contractId)
      stakeholders = storedContract.contract.metadata.stakeholders

      timeProofAndSnapshot <- transferCoordination.getTimeProofAndSnapshot(targetDomain)
      (timeProof, targetCrypto) = timeProofAndSnapshot
      _ = logger.debug(withDetails(s"Picked time proof ${timeProof.timestamp}"))

      transferCounter <- EitherT(
        ephemeralState.tracker
          .getApproximateStates(Seq(contractId))
          .map(_.get(contractId) match {
            case Some(state) =>
              state.status match {
                case Active(tc) => Right(tc)
                case Archived | _: TransferredAway =>
                  Left(
                    TransferOutProcessorError.DeactivatedContract(contractId, status = state.status)
                  )
              }
            case None => Left(TransferOutProcessorError.UnknownContract(contractId))
          })
      ).mapK(FutureUnlessShutdown.outcomeK)

      newTransferCounter <- EitherT.fromEither[FutureUnlessShutdown](
        transferCounter.increment
          .leftMap(_ => TransferOutProcessorError.TransferCounterOverflow)
      )

      creatingTransactionId <- EitherT.fromEither[FutureUnlessShutdown](
        storedContract.creatingTransactionIdO.toRight(CreatingTransactionIdNotFound(contractId))
      )

      validated <- TransferOutRequest.validated(
        participantId,
        timeProof,
        creatingTransactionId,
        storedContract.contract,
        submitterMetadata,
        stakeholders,
        domainId,
        sourceDomainProtocolVersion,
        mediator,
        targetDomain,
        targetProtocolVersion,
        sourceRecentSnapshot.ipsSnapshot,
        targetCrypto.ipsSnapshot,
        newTransferCounter,
        logger,
      )

      transferOutUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()
      fullTree = validated.request.toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        transferOutUuid,
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- sourceRecentSnapshot
        .sign(rootHash.unwrap)
        .leftMap(TransferSigningError)
        .mapK(FutureUnlessShutdown.outcomeK)
      mediatorMessage = fullTree.mediatorMessage(submittingParticipantSignature)
      viewMessage <- EncryptedViewMessageFactory
        .create(TransferOutViewType)(
          fullTree,
          sourceRecentSnapshot,
          ephemeralState.sessionKeyStoreLookup,
          sourceDomainProtocolVersion.v,
        )
        .leftMap[TransferProcessorError](EncryptionError(contractId, _))
        .mapK(FutureUnlessShutdown.outcomeK)
      maybeRecipients = Recipients.ofSet(validated.recipients)
      recipientsT <- EitherT
        .fromOption[FutureUnlessShutdown](
          maybeRecipients,
          NoStakeholders.logAndCreate(contractId, logger): TransferProcessorError,
        )
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId.unwrap,
          sourceDomainProtocolVersion.v,
          ViewType.TransferOutViewType,
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
      TransferSubmission(Batch.of(sourceDomainProtocolVersion.v, messages*), rootHash)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, TransferProcessorError, SubmissionResultArgs] = {
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      None,
      submissionParam.submittingParty,
      pendingSubmissionId,
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult = {
    val requestId = RequestId(deliver.timestamp)
    val transferId = TransferId(domainId, requestId.unwrap)
    SubmissionResult(transferId, pendingSubmission.transferCompletion.future)
  }

  private[this] def getStoredContract(
      contractLookup: ContractLookup,
      contractId: LfContractId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, StoredContract] =
    contractLookup
      .lookup(contractId)
      .toRight[TransferProcessorError](TransferOutProcessorError.UnknownContract(contractId))
      .mapK(FutureUnlessShutdown.outcomeK)

  override protected def decryptTree(
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[TransferOutViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[Future, EncryptedViewMessageError, WithRecipients[
    FullTransferOutTree
  ]] = {
    EncryptedViewMessage
      .decryptFor(
        sourceSnapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullTransferOutTree
          .fromByteString(sourceSnapshot.pureCrypto, sourceDomainProtocolVersion)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(WithRecipients(_, envelope.recipients))
  }

  private def expectedDomainId(
      fromRequest: SourceDomainId,
      timestamp: CantonTimestamp,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      fromRequest == domainId,
      UnexpectedDomain(
        TransferId(fromRequest, timestamp),
        domainId.unwrap,
      ),
    )

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      fullViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[FullTransferOutTree], Option[Signature])]
      ],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
      mediator: MediatorsOfDomain,
      // not actually used here, because it's available in the only fully unblinded view
      submitterMetadataO: Option[ViewSubmitterMetadata],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CheckActivenessAndWritePendingContracts] = {
    val correctRootHashes = fullViewsWithSignatures.map { case (rootHashes, _) => rootHashes }
    // TODO(i12926): Send a rejection if malformedPayloads is non-empty
    for {
      txOutRequestAndRecipients <- EitherT.cond[Future](
        correctRootHashes.toList.sizeCompare(1) == 0,
        correctRootHashes.head1,
        ReceivedMultipleRequests(correctRootHashes.map(_.unwrap.viewHash)): TransferProcessorError,
      )
      WithRecipients(txOutRequest, recipients) = txOutRequestAndRecipients
      contractId = txOutRequest.contractId
      _ <- expectedDomainId(txOutRequest.sourceDomain, ts).onShutdown(
        Left(TransferOutProcessorError.AbortedDueToShutdownOut(txOutRequest.contractId))
      )
      contractIdS = Set(contractId)
      contractsCheck = ActivenessCheck.tryCreate(
        checkFresh = Set.empty,
        checkFree = Set.empty,
        checkActive = contractIdS,
        lock = contractIdS,
        needPriorState = contractIdS,
      )
      activenessSet = ActivenessSet(
        contracts = contractsCheck,
        transferIds = Set.empty,
      )
    } yield CheckActivenessAndWritePendingContracts(
      activenessSet,
      PendingDataAndResponseArgs(txOutRequest, recipients, ts, rc, sc, sourceSnapshot),
    )
  }

  /** Wait until the participant has received and processed all topology transactions on the target domain
    * up to the target-domain time proof timestamp.
    *
    * As we're not processing messages in parallel, delayed message processing on one domain can
    * block message processing on another domain and thus breaks isolation across domains.
    * Even with parallel processing, the cursors in the request journal would not move forward,
    * so event emission to the event log blocks, too.
    *
    * No deadlocks can arise under normal behaviour though.
    * For a deadlock, we would need cyclic waiting, i.e., a transfer-out request on one domain D1 references
    * a time proof on another domain D2 and a earlier transfer-out request on D2 references a time proof on D3
    * and so on to domain Dn and an earlier transfer-out request on Dn references a later time proof on D1.
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
      transferringParticipant: Boolean,
      domainId: TargetDomainId,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Option[TopologySnapshot]] =
    Option
      .when(transferringParticipant) {
        transferCoordination
          .awaitTimestampAndGetCryptoSnapshot(
            domainId.unwrap,
            timestamp,
            waitForEffectiveTime = true,
          )
      }
      .traverse(_.map(_.ipsSnapshot))

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      activenessF: FutureUnlessShutdown[ActivenessResult],
      mediator: MediatorsOfDomain,
      freshOwnTimelyTx: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val PendingDataAndResponseArgs(fullTree, recipients, ts, rc, sc, sourceSnapshot) =
      pendingDataAndResponseArgs

    val transferId: TransferId = TransferId(fullTree.sourceDomain, ts)
    val view = fullTree.tree.view.tryUnwrap
    for {
      // Since the transfer out request should be sent only to participants that host a stakeholder of the contract,
      // we can expect to find the contract in the contract store.
      contractWithTransactionId <-
        // TODO(i15090): Validate contract data against contract id and contract metadata against contract data
        EitherT.rightT[FutureUnlessShutdown, TransferProcessorError](
          WithTransactionId(view.contract, view.creatingTransactionId)
        )

      WithTransactionId(contract, creatingTransactionId) = contractWithTransactionId

      transferringParticipant = fullTree.adminParties.contains(participantId.adminParty.toLf)

      targetTopology <- getTopologySnapshotAtTimestamp(
        transferringParticipant,
        fullTree.targetDomain,
        fullTree.targetTimeProof.timestamp,
      )

      _ <- TransferOutValidation(
        fullTree,
        contract.metadata.stakeholders,
        contract.rawContractInstance.contractInstance.unversioned.template,
        sourceDomainProtocolVersion,
        sourceSnapshot.ipsSnapshot,
        targetTopology,
        recipients,
        logger,
      )

      transferInExclusivity <- getTransferInExclusivity(
        targetTopology,
        fullTree.targetTimeProof.timestamp,
        fullTree.targetDomain,
      )

      activenessResult <- EitherT.right(activenessF)

      hostedStks <- EitherT.liftF(
        FutureUnlessShutdown.outcomeF(
          hostedStakeholders(fullTree.stakeholders.toList, sourceSnapshot.ipsSnapshot)
        )
      )

      requestId = RequestId(ts)
      entry = PendingTransferOut(
        requestId,
        rc,
        sc,
        fullTree.tree.rootHash,
        WithContractHash.fromContract(contract, fullTree.contractId),
        fullTree.transferCounter,
        contract.rawContractInstance.contractInstance.unversioned.template,
        contract.rawContractInstance.contractInstance.unversioned.packageName,
        transferringParticipant,
        fullTree.submitterMetadata,
        transferId,
        fullTree.targetDomain,
        fullTree.stakeholders,
        hostedStks.toSet,
        fullTree.targetTimeProof,
        transferInExclusivity,
        mediator,
      )

      transferOutDecisionTime <- ProcessingSteps
        .getDecisionTime(sourceSnapshot.ipsSnapshot, ts)
        .leftMap(TransferParametersError(domainId.unwrap, _))
        .mapK(FutureUnlessShutdown.outcomeK)

      transferData = TransferData(
        sourceProtocolVersion = sourceDomainProtocolVersion,
        transferOutTimestamp = ts,
        transferOutRequestCounter = rc,
        transferOutRequest = fullTree,
        transferOutDecisionTime = transferOutDecisionTime,
        contract = contract,
        creatingTransactionId = creatingTransactionId,
        transferOutResult = None,
        transferGlobalOffset = None,
      )
      _ <- ifThenET(transferringParticipant) {
        transferCoordination.addTransferOutRequest(transferData).mapK(FutureUnlessShutdown.outcomeK)
      }
      confirmingStakeholders <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          sourceSnapshot.ipsSnapshot.canConfirm(
            participantId,
            contract.metadata.stakeholders,
          )
        )
      )
      responseOpt = createTransferOutResponse(
        requestId,
        transferringParticipant,
        activenessResult,
        contract.contractId,
        fullTree.transferCounter,
        confirmingStakeholders,
        fullTree.tree.rootHash,
      )
    } yield StorePendingDataAndSendResponseAndCreateTimeout(
      entry,
      responseOpt.map(_ -> Recipients.cc(mediator)).toList,
      RejectionArgs(
        entry,
        LocalRejectError.TimeRejects.LocalTimeout
          .Reject()
          .toLocalReject(sourceDomainProtocolVersion.v),
      ),
    )
  }

  private[this] def getTransferInExclusivity(
      targetTopology: Option[TopologySnapshot],
      timestamp: CantonTimestamp,
      domainId: TargetDomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Option[CantonTimestamp]] =
    targetTopology.traverse(
      ProcessingSteps
        .getTransferInExclusivity(_, timestamp)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap(TransferParametersError(domainId.unwrap, _))
    )

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: PendingTransferOut,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val PendingTransferOut(
      requestId,
      requestCounter,
      requestSequencerCounter,
      rootHash,
      WithContractHash(contractId, contractHash),
      transferCounter,
      templateId,
      packageName,
      transferringParticipant,
      submitterMetadata,
      transferId,
      targetDomain,
      stakeholders,
      hostedStakeholders,
      _targetTimeProof,
      transferInExclusivity,
      _mediatorId,
    ) = pendingRequestData

    val pendingSubmissionData = pendingSubmissionMap.get(rootHash)

    def rejected(
        reason: TransactionRejection
    ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = for {
      _ <- ifThenET(transferringParticipant) { deleteTransfer(targetDomain, requestId) }

      eventO <- EitherT.fromEither[Future](
        createRejectionEvent(RejectionArgs(pendingRequestData, reason))
      )
    } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    verdict match {
      case _: Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          transferOuts = Map(
            contractId -> WithContractHash(
              CommitSet.TransferOutCommit(targetDomain, stakeholders, transferCounter),
              contractHash,
            )
          ),
          transferIns = Map.empty,
        )
        val commitSetFO = Some(Future.successful(commitSet))
        for {
          _ <- ifThenET(transferringParticipant) {
            EitherT
              .fromEither[Future](DeliveredTransferOutResult.create(event))
              .leftMap(err => TransferOutProcessorError.InvalidResult(transferId, err))
              .flatMap(deliveredResult =>
                transferCoordination.addTransferOutResult(targetDomain, deliveredResult)
              )
          }

          notInitiator = pendingSubmissionData.isEmpty
          _ <-
            if (notInitiator && transferringParticipant)
              triggerTransferInWhenExclusivityTimeoutExceeded(pendingRequestData)
            else EitherT.pure[Future, TransferProcessorError](())

          transferOutEvent <- createTransferredOut(
            contractId,
            templateId,
            packageName,
            stakeholders,
            submitterMetadata,
            transferId,
            targetDomain,
            rootHash,
            transferInExclusivity,
            isTransferringParticipant = transferringParticipant,
            transferCounter,
            hostedStakeholders.toList,
          )
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetFO,
          Seq.empty,
          Some(
            TimestampedEvent(
              transferOutEvent,
              RequestOffset(requestId.unwrap, requestCounter),
              Some(requestSequencerCounter),
            )
          ),
        )

      case reasons: Verdict.ParticipantReject => rejected(reasons.keyEvent)

      case rejection: MediatorReject => rejected(rejection)
    }
  }

  private def createTransferredOut(
      contractId: LfContractId,
      templateId: LfTemplateId,
      packageName: LfPackageName,
      contractStakeholders: Set[LfPartyId],
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      targetDomain: TargetDomainId,
      rootHash: RootHash,
      transferInExclusivity: Option[CantonTimestamp],
      isTransferringParticipant: Boolean,
      transferCounter: TransferCounter,
      hostedStakeholders: List[LfPartyId],
  ): EitherT[Future, TransferProcessorError, LedgerSyncEvent.TransferredOut] = {
    for {
      updateId <- EitherT
        .fromEither[Future](rootHash.asLedgerTransactionId)
        .leftMap[TransferProcessorError](FieldConversionError(transferId, "Transaction Id", _))

      completionInfo =
        Option.when(participantId == submitterMetadata.submittingParticipant)(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            applicationId = submitterMetadata.applicationId,
            commandId = submitterMetadata.commandId,
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
            statistics = None,
          )
        )
    } yield LedgerSyncEvent.TransferredOut(
      updateId = updateId,
      optCompletionInfo = completionInfo,
      submitter = Option(submitterMetadata.submitter),
      contractId = contractId,
      templateId = Some(templateId),
      packageName = packageName,
      contractStakeholders = contractStakeholders,
      transferId = transferId,
      targetDomain = targetDomain,
      transferInExclusivity = transferInExclusivity.map(_.toLf),
      workflowId = submitterMetadata.workflowId,
      isTransferringParticipant = isTransferringParticipant,
      hostedStakeholders = hostedStakeholders,
      transferCounter = transferCounter,
    )
  }

  private[this] def triggerTransferInWhenExclusivityTimeoutExceeded(
      pendingRequestData: RequestType#PendingRequestData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {

    val targetDomain = pendingRequestData.targetDomain
    val t0 = pendingRequestData.targetTimeProof.timestamp

    AutomaticTransferIn.perform(
      pendingRequestData.transferId,
      targetDomain,
      transferCoordination,
      pendingRequestData.stakeholders,
      pendingRequestData.submitterMetadata,
      participantId,
      t0,
    )
  }

  private[this] def deleteTransfer(targetDomain: TargetDomainId, transferOutRequestId: RequestId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val transferId = TransferId(domainId, transferOutRequestId.unwrap)
    transferCoordination.deleteTransfer(targetDomain, transferId)
  }

  private[this] def createTransferOutResponse(
      requestId: RequestId,
      transferringParticipant: Boolean,
      activenessResult: ActivenessResult,
      contractId: LfContractId,
      declaredTransferCounter: TransferCounter,
      confirmingStakeholders: Set[LfPartyId],
      rootHash: RootHash,
  ): Option[ConfirmationResponse] = {
    val expectedPriorTransferCounter = Map[LfContractId, Option[ActiveContractStore.Status]](
      contractId -> Some(ActiveContractStore.Active(declaredTransferCounter - 1))
    )

    val successful =
      declaredTransferCounter > TransferCounter.Genesis &&
        activenessResult.isSuccessful &&
        activenessResult.contracts.priorStates == expectedPriorTransferCounter
    // send a response only if the participant is a transferring participant or the activeness check has failed
    if (transferringParticipant || !successful) {
      val adminPartySet =
        if (transferringParticipant) Set(participantId.adminParty.toLf) else Set.empty[LfPartyId]
      val confirmingParties = confirmingStakeholders union adminPartySet
      val localVerdict =
        if (successful) LocalApprove(sourceDomainProtocolVersion.v)
        else
          LocalRejectError.TransferOutRejects.ActivenessCheckFailed
            .Reject(s"$activenessResult")
            .toLocalReject(sourceDomainProtocolVersion.v)
      val response = checked(
        ConfirmationResponse.tryCreate(
          requestId,
          participantId,
          Some(ViewPosition.root),
          localVerdict,
          rootHash,
          confirmingParties,
          domainId.unwrap,
          sourceDomainProtocolVersion.v,
        )
      )
      Some(response)
    } else None
  }
}

object TransferOutProcessingSteps {

  final case class SubmissionParam(
      submitterMetadata: TransferSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
  ) {
    val submittingParty: LfPartyId = submitterMetadata.submitter
  }

  final case class SubmissionResult(
      transferId: TransferId,
      transferOutCompletionF: Future[com.google.rpc.status.Status],
  )

  final case class PendingTransferOut(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contractIdAndHash: WithContractHash[LfContractId],
      transferCounter: TransferCounter,
      templateId: LfTemplateId,
      packageName: LfPackageName,
      transferringParticipant: Boolean,
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      targetDomain: TargetDomainId,
      stakeholders: Set[LfPartyId],
      hostedStakeholders: Set[LfPartyId],
      targetTimeProof: TimeProof,
      transferInExclusivity: Option[CantonTimestamp],
      mediator: MediatorsOfDomain,
  ) extends PendingTransfer
      with PendingRequestData {

    override def rootHashO: Option[RootHash] = Some(rootHash)
  }

  final case class PendingDataAndResponseArgs(
      txOutRequest: FullTransferOutTree,
      recipients: Recipients,
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
  )

}
