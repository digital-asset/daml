// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.daml.lf.data.Bytes
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DecryptionError as _, EncryptionError as _, *}
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.ledger.participant.state.CompletionInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
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
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{
  CanSubmitTransfer,
  EngineController,
  ProcessingSteps,
}
import com.digitalasset.canton.participant.store.ActiveContractStore.Archived
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.ConfirmationResponse.InvalidConfirmationResponse
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  TransferCounter,
  checked,
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[transfer] class TransferInProcessingSteps(
    val domainId: TargetDomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    staticDomainParameters: StaticDomainParameters,
    targetProtocolVersion: TargetProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferInViewType,
      PendingTransferIn,
    ]
    with NamedLogging {

  import TransferInProcessingSteps.*

  override def requestKind: String = "TransferIn"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submitterMetadata.submitter}, transferId ${param.transferId}"

  override type SubmissionResultArgs = PendingTransferSubmission

  override type RequestType = ProcessingSteps.RequestType.TransferIn
  override val requestType = ProcessingSteps.RequestType.TransferIn

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions = {
    state.pendingTransferInSubmissions
  }

  private val transferInValidation = new TransferInValidation(
    domainId,
    staticDomainParameters,
    participantId,
    engine,
    transferCoordination,
    loggerFactory,
  )

  override def submissionIdOfPendingRequest(pendingData: PendingTransferIn): RootHash =
    pendingData.rootHash

  override def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Submission] = {

    val SubmissionParam(
      submitterMetadata,
      transferId,
      sourceProtocolVersion,
    ) = submissionParam
    val topologySnapshot = recentSnapshot.ipsSnapshot
    val pureCrypto = recentSnapshot.pureCrypto
    val submitter = submitterMetadata.submitter

    def activeParticipantsOfParty(
        parties: Seq[LfPartyId]
    ): EitherT[Future, TransferProcessorError, Set[ParticipantId]] = EitherT(
      topologySnapshot.activeParticipantsOfPartiesWithAttributes(parties).map {
        partyToParticipantAttributes =>
          import cats.syntax.traverse.*
          partyToParticipantAttributes.toSeq
            .traverse { case (party, participants) =>
              Either.cond(
                participants.nonEmpty,
                participants.keySet,
                NoParticipantForReceivingParty(transferId, party),
              )
            }
            .map(_.toSet.flatten)
      }
    )

    for {
      transferData <- ephemeralState.transferLookup
        .lookup(transferId)
        .leftMap(err => NoTransferData(transferId, err))
        .mapK(FutureUnlessShutdown.outcomeK)
      transferOutResult <- EitherT.fromEither[FutureUnlessShutdown](
        transferData.transferOutResult.toRight(TransferOutIncomplete(transferId, participantId))
      )

      targetDomain = transferData.targetDomain
      _ = if (targetDomain != domainId)
        throw new IllegalStateException(
          s"Transfer-in $transferId: Transfer data for ${transferData.targetDomain} found on wrong domain $domainId"
        )

      stakeholders = transferData.transferOutRequest.stakeholders
      _ <- condUnitET[FutureUnlessShutdown](
        stakeholders.contains(submitter),
        SubmittingPartyMustBeStakeholderIn(transferId, submitter, stakeholders),
      )

      _ <- CanSubmitTransfer
        .transferIn(transferId, topologySnapshot, submitter, participantId)
        .mapK(FutureUnlessShutdown.outcomeK)

      transferInUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()

      fullTree <- EitherT.fromEither[FutureUnlessShutdown](
        makeFullTransferInTree(
          pureCrypto,
          seed,
          submitterMetadata,
          stakeholders,
          transferData.contract,
          transferData.transferCounter,
          transferData.creatingTransactionId,
          targetDomain,
          mediator,
          transferOutResult,
          transferInUuid,
          sourceProtocolVersion,
          targetProtocolVersion,
        )
      )

      rootHash = fullTree.rootHash
      submittingParticipantSignature <- recentSnapshot
        .sign(rootHash.unwrap)
        .leftMap(TransferSigningError)
      mediatorMessage = fullTree.mediatorMessage(submittingParticipantSignature)
      recipientsSet <- activeParticipantsOfParty(stakeholders.toSeq).mapK(
        FutureUnlessShutdown.outcomeK
      )
      recipients <- EitherT.fromEither[FutureUnlessShutdown](
        Recipients
          .ofSet(recipientsSet)
          .toRight(NoStakeholders.logAndCreate(transferData.contract.contractId, logger))
      )
      viewMessage <- EncryptedViewMessageFactory
        .create(TransferInViewType)(
          fullTree,
          recentSnapshot,
          ephemeralState.sessionKeyStoreLookup,
          targetProtocolVersion.v,
        )
        .leftMap[TransferProcessorError](EncryptionError(transferData.contract.contractId, _))
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId.unwrap,
          targetProtocolVersion.v,
          ViewType.TransferInViewType,
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
      TransferSubmission(Batch.of(targetProtocolVersion.v, messages*), rootHash)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      submissionId: PendingSubmissionId,
  ): EitherT[Future, TransferProcessorError, SubmissionResultArgs] = {
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      Some(submissionParam.transferId),
      submissionParam.submitterLf,
      submissionId,
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult =
    SubmissionResult(pendingSubmission.transferCompletion.future)

  override protected def decryptTree(
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[TransferInViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, WithRecipients[
    FullTransferInTree
  ]] =
    EncryptedViewMessage
      .decryptFor(
        staticDomainParameters,
        snapshot,
        sessionKeyStore,
        envelope.protocolMessage,
        participantId,
      ) { bytes =>
        FullTransferInTree
          .fromByteString(snapshot.pureCrypto, targetProtocolVersion)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(WithRecipients(_, envelope.recipients))

  override def computeActivenessSet(
      parsedRequest: ParsedTransferRequest[FullTransferInTree]
  )(implicit
      traceContext: TraceContext
  ): Either[TransferProcessorError, ActivenessSet] =
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
        transferIds =
          if (parsedRequest.transferringParticipant)
            Set(parsedRequest.fullViewTree.transferOutResultEvent.transferId)
          else Set.empty,
      )
      Right(activenessSet)
    } else
      Left(
        UnexpectedDomain(
          parsedRequest.fullViewTree.transferOutResultEvent.transferId,
          targetDomain = parsedRequest.fullViewTree.domainId,
          receivedOn = domainId.unwrap,
        )
      )

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedRequestType,
      transferLookup: TransferLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {

    val ParsedTransferRequest(
      rc,
      ts,
      sc,
      fullViewTree,
      _,
      _,
      _,
      _,
      transferringParticipant,
      _,
      mediator,
      targetCrypto,
      _,
    ) = parsedRequest

    val requestId = RequestId(ts)
    val transferId = fullViewTree.transferOutResultEvent.transferId

    // We perform the stakeholders check asynchronously so that we can complete the pending request
    // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
    // mediator verdict.
    val stakeholdersCheckResultET = transferInValidation
      .checkStakeholders(
        fullViewTree,
        getEngineAbortStatus = () => engineController.abortStatus,
      )
      .mapK(FutureUnlessShutdown.outcomeK)

    for {
      hostedStks <- EitherT.right[TransferProcessorError](
        FutureUnlessShutdown.outcomeF(
          hostedStakeholders(
            fullViewTree.contract.metadata.stakeholders.toList,
            targetCrypto.ipsSnapshot,
          )
        )
      )

      transferDataO <- EitherT
        .right[TransferProcessorError](
          transferLookup.lookup(transferId).toOption.value
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      validationResultO <- transferInValidation
        .validateTransferInRequest(
          ts,
          fullViewTree,
          transferDataO,
          targetCrypto,
          transferringParticipant,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      activenessResult <- EitherT.right[TransferProcessorError](activenessResultFuture)
    } yield {
      val responsesET = for {
        _ <- stakeholdersCheckResultET
        transferResponses <- EitherT
          .fromEither[FutureUnlessShutdown](
            createConfirmationResponses(
              requestId,
              fullViewTree,
              activenessResult,
              validationResultO,
            )
          )
          .leftMap(e => FailedToCreateResponse(transferId, e): TransferProcessorError)
      } yield {
        transferResponses.map(_ -> Recipients.cc(mediator))
      }

      // We consider that we rejected if we fail to process or if at least one of the responses is not "approve'
      val locallyRejectedF = responsesET.value.map(
        _.fold(
          _ => true,
          _.exists { case (response, _) =>
            !response.localVerdict.isApprove
          },
        )
      )
      val engineAbortStatusF = stakeholdersCheckResultET.value.map {
        case Left(ReinterpretationAborted(_, reason)) => EngineAbortStatus.aborted(reason)
        case _ => EngineAbortStatus.notAborted
      }

      // construct pending data and response
      val entry = PendingTransferIn(
        requestId,
        rc,
        sc,
        fullViewTree.rootHash,
        fullViewTree.contract,
        fullViewTree.transferCounter,
        fullViewTree.submitterMetadata,
        fullViewTree.creatingTransactionId,
        transferringParticipant,
        transferId,
        hostedStks.toSet,
        mediator,
        locallyRejectedF,
        engineController.abort,
        engineAbortStatusF,
      )

      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        responsesET,
        RejectionArgs(
          entry,
          LocalRejectError.TimeRejects.LocalTimeout.Reject().toLocalReject(targetProtocolVersion.v),
        ),
      )
    }
  }

  private def createConfirmationResponses(
      requestId: RequestId,
      txInRequest: FullTransferInTree,
      activenessResult: ActivenessResult,
      validationResultO: Option[TransferInValidationResult],
  )(implicit
      traceContext: TraceContext
  ): Either[InvalidConfirmationResponse, Seq[ConfirmationResponse]] =
    validationResultO match {
      case None => Right(Seq.empty[ConfirmationResponse])

      case Some(validationResult) =>
        val contractResult = activenessResult.contracts

        val localRejectErrorO =
          if (activenessResult.isSuccessful)
            None
          else if (contractResult.notFree.nonEmpty) {
            contractResult.notFree.toSeq match {
              case Seq((coid, Archived)) =>
                Some(
                  LocalRejectError.TransferInRejects.ContractAlreadyArchived
                    .Reject(show"coid=$coid")
                )
              case Seq((coid, _state)) =>
                Some(
                  LocalRejectError.TransferInRejects.ContractAlreadyActive
                    .Reject(show"coid=$coid")
                )
              case coids =>
                throw new RuntimeException(
                  s"Activeness result for a transfer-in fails for multiple contract IDs $coids"
                )
            }
          } else if (contractResult.alreadyLocked.nonEmpty)
            Some(
              LocalRejectError.TransferInRejects.ContractIsLocked
                .Reject("")
            )
          else if (activenessResult.inactiveTransfers.nonEmpty)
            Some(
              LocalRejectError.TransferInRejects.AlreadyCompleted
                .Reject("")
            )
          else
            throw new RuntimeException(
              withRequestId(requestId, s"Unexpected activeness result $activenessResult")
            )

        val localVerdict =
          localRejectErrorO.fold[LocalVerdict](LocalApprove(targetProtocolVersion.v)) { err =>
            err.logWithContext()
            err.toLocalReject(targetProtocolVersion.v)
          }

        ConfirmationResponse
          .create(
            requestId,
            participantId,
            Some(ViewPosition.root),
            localVerdict,
            txInRequest.rootHash,
            validationResult.confirmingParties,
            domainId.id,
            targetProtocolVersion.v,
          )
          .map(transferResponse => Seq(transferResponse))

    }

  private[this] def withRequestId(requestId: RequestId, message: String) =
    s"Transfer-in $requestId: $message"

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: PendingTransferIn,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    CommitAndStoreContractsAndPublishEvent,
  ] = {
    val PendingTransferIn(
      requestId,
      requestCounter,
      requestSequencerCounter,
      rootHash,
      contract,
      transferCounter,
      submitterMetadata,
      creatingTransactionId,
      transferringParticipant,
      transferId,
      hostedStakeholders,
      _,
      _locallyRejectedF,
      _engineController,
      _abortedF,
    ) = pendingRequestData

    def rejected(
        reason: TransactionRejection
    ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = for {
      eventO <- EitherT.fromEither[Future](
        createRejectionEvent(RejectionArgs(pendingRequestData, reason))
      )
    } yield CommitAndStoreContractsAndPublishEvent(None, Seq.empty, eventO)

    verdict match {
      case _: Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          transferOuts = Map.empty,
          transferIns = Map(
            contract.contractId -> WithContractHash
              .fromContract(
                contract,
                CommitSet.TransferInCommit(
                  transferId,
                  contract.metadata,
                  transferCounter,
                ),
              )
          ),
        )
        val commitSetO = Some(Future.successful(commitSet))
        val contractsToBeStored = Seq(WithTransactionId(contract, creatingTransactionId))

        for {
          event <- createTransferredIn(
            contract,
            creatingTransactionId,
            requestId.unwrap,
            submitterMetadata,
            transferId,
            rootHash,
            isTransferringParticipant = transferringParticipant,
            transferCounter,
            hostedStakeholders.toList,
          )
          timestampEvent = Some(
            TimestampedEvent(
              event,
              RequestOffset(requestId.unwrap, requestCounter),
              Some(requestSequencerCounter),
            )
          )
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetO,
          contractsToBeStored,
          timestampEvent,
        )

      case reasons: Verdict.ParticipantReject => rejected(reasons.keyEvent)

      case rejection: Verdict.MediatorReject => rejected(rejection)
    }
  }.mapK(FutureUnlessShutdown.outcomeK)

  private[transfer] def createTransferredIn(
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      recordTime: CantonTimestamp,
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      rootHash: RootHash,
      isTransferringParticipant: Boolean,
      transferCounter: TransferCounter,
      hostedStakeholders: List[LfPartyId],
  ): EitherT[Future, TransferProcessorError, LedgerSyncEvent.TransferredIn] = {
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
        DriverContractMetadata(salt).toLfBytes(targetProtocolVersion.v)
      }
      .getOrElse(Bytes.Empty)

    for {
      updateId <- EitherT.fromEither[Future](
        rootHash.asLedgerTransactionId.leftMap[TransferProcessorError](
          FieldConversionError(transferId, "Transaction id (root hash)", _)
        )
      )

      ledgerCreatingTransactionId <- EitherT.fromEither[Future](
        creatingTransactionId.asLedgerTransactionId.leftMap[TransferProcessorError](
          FieldConversionError(transferId, "Transaction id (creating transaction)", _)
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
            statistics = None,
          )
        )
    } yield LedgerSyncEvent.TransferredIn(
      updateId = updateId,
      optCompletionInfo = completionInfo,
      submitter = Option(submitterMetadata.submitter),
      recordTime = recordTime.toLf,
      ledgerCreateTime = contract.ledgerCreateTime.toLf,
      createNode = createNode,
      creatingTransactionId = ledgerCreatingTransactionId,
      contractMetadata = driverContractMetadata,
      transferId = transferId,
      targetDomain = targetDomain,
      workflowId = submitterMetadata.workflowId,
      isTransferringParticipant = isTransferringParticipant,
      hostedStakeholders = hostedStakeholders,
      transferCounter = transferCounter,
    )
  }
}

object TransferInProcessingSteps {

  final case class SubmissionParam(
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      sourceProtocolVersion: SourceProtocolVersion,
  ) {
    val submitterLf: LfPartyId = submitterMetadata.submitter
  }

  final case class SubmissionResult(transferInCompletionF: Future[com.google.rpc.status.Status])

  final case class PendingTransferIn(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contract: SerializableContract,
      transferCounter: TransferCounter,
      submitterMetadata: TransferSubmitterMetadata,
      creatingTransactionId: TransactionId,
      isTransferringParticipant: Boolean,
      transferId: TransferId,
      hostedStakeholders: Set[LfPartyId],
      mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends PendingTransfer {

    override def rootHashO: Option[RootHash] = Some(rootHash)
  }

  private[transfer] def makeFullTransferInTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      submitterMetadata: TransferSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      transferCounter: TransferCounter,
      creatingTransactionId: TransactionId,
      targetDomain: TargetDomainId,
      targetMediator: MediatorGroupRecipient,
      transferOutResult: DeliveredTransferOutResult,
      transferInUuid: UUID,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): Either[TransferProcessorError, FullTransferInTree] = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)

    val commonData = TransferInCommonData
      .create(pureCrypto)(
        commonDataSalt,
        targetDomain,
        targetMediator,
        stakeholders,
        transferInUuid,
        submitterMetadata,
        targetProtocolVersion,
      )

    for {
      view <- TransferInView
        .create(pureCrypto)(
          viewSalt,
          contract,
          creatingTransactionId,
          transferOutResult,
          sourceProtocolVersion,
          targetProtocolVersion,
          transferCounter,
        )
        .leftMap(reason => InvalidTransferView(reason))
      tree = TransferInViewTree(commonData, view, targetProtocolVersion, pureCrypto)
    } yield FullTransferInTree(tree)
  }
}
