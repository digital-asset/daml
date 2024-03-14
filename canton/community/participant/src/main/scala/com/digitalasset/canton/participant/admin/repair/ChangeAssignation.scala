// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.data.Bytes
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.ChangeAssignation.Changed
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*

import scala.concurrent.{ExecutionContext, Future}

private final class ChangeAssignation(
    contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
    repairSource: RepairRequest,
    repairTarget: RepairRequest,
    skipInactive: Boolean,
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiProvider,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val sourceDomainId = SourceDomainId(repairSource.domain.id)
  private val targetDomainId = TargetDomainId(repairTarget.domain.id)
  private val transferId = TransferId(sourceDomainId, repairSource.timestamp)

  /** Change the domain assignation for contracts from [[repairSource]] to [[repairTarget]]
    */
  private def run()(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    for {
      contractStatusAtSource <- EitherT.right(
        repairSource.domain.persistentState.activeContractStore
          .fetchStates(contractIds.map(_.payload))
      )
      _ = logger.debug(s"Contracts status at source: $contractStatusAtSource")
      contractsAtSource <- changingContractsAtSource(contractStatusAtSource)
      sourceContractIds = contractsAtSource.map(_.payload._1)
      _ = logger.debug(s"Contracts changing assignation from source: $sourceContractIds")
      contractStatusAtTarget <- EitherT.right(
        repairTarget.domain.persistentState.activeContractStore.fetchStates(sourceContractIds)
      )
      _ = logger.debug(s"Contract status at target: $contractStatusAtTarget")
      contractIds <- changingContractIds(contractsAtSource, contractStatusAtTarget)
      _ = logger.debug(s"Contracts changing assignation: $contractIds")
      contracts <- readContracts(contractIds)
      _ = logger.debug(
        s"Contracts that need to change assignation with persistence status: $contracts"
      )
      transactionId = randomTransactionId(syncCrypto)
      _ <- persistContracts(transactionId, contracts)
      _ <- persistTransferOutAndIn(contracts).toEitherT
      _ <- insertTransferEventsInLog(transactionId, contracts)
    } yield ()

  private def changingContractsAtSource(
      source: Map[LfContractId, ContractState]
  )(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, String, List[ChangeAssignation.Data[(LfContractId, TransferCounter)]]] =
    EitherT.fromEither(
      contractIds
        .map(cid => (cid, source.get(cid.payload).map(_.status)))
        .toList
        .traverse {
          case (cid, None) =>
            Either.cond(
              skipInactive,
              None,
              s"Contract $cid does not exist in source domain and cannot be moved.",
            )
          case (cid, Some(ActiveContractStore.Active(transferCounter))) =>
            Right(Some(cid.copy(payload = (cid.payload, transferCounter))))
          case (cid, Some(ActiveContractStore.Archived)) =>
            Either.cond(
              skipInactive,
              None,
              s"Contract $cid has been archived and cannot be moved.",
            )
          case (cid, Some(ActiveContractStore.TransferredAway(target, _transferCounter))) =>
            Either
              .cond(
                skipInactive,
                None,
                s"Contract $cid has been transferred to $target and cannot be moved.",
              )
        }
        .map(_.flatten)
    )

  private def changingContractIds(
      sourceContracts: List[ChangeAssignation.Data[(LfContractId, TransferCounter)]],
      targetStatus: Map[LfContractId, ContractState],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[ChangeAssignation.Data[(LfContractId, TransferCounter)]]] = {
    val filteredE =
      sourceContracts
        .traverse { case data @ ChangeAssignation.Data((cid, transferCounter), _, _) =>
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)
          targetStatusOfContract match {
            case None | Some(ActiveContractStore.TransferredAway(_, _)) =>
              transferCounter.increment
                .map(incrementedTc => data.copy(payload = (cid, incrementedTc)))
            case Some(targetState) =>
              Left(
                s"Active contract $cid in source domain exists in target domain with status $targetState. Use 'repair.add' or 'repair.purge' instead."
              )
          }
        }

    for {
      filtered <- EitherT.fromEither[Future](filteredE)
      filteredContractIds = filtered.map(_.payload._1)
      stakeholders <- stakeholdersAtSource(filteredContractIds.toSet)
      _ <- filteredContractIds.parTraverse_ { contractId =>
        atLeastOneHostedStakeholderAtTarget(
          contractId,
          stakeholders.getOrElse(contractId, Set.empty),
        )
      }
    } yield filtered
  }

  private def stakeholdersAtSource(contractIds: Set[LfContractId])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Map[LfContractId, Set[LfPartyId]]] =
    repairSource.domain.persistentState.contractStore
      .lookupStakeholders(contractIds)
      .leftMap(e =>
        s"Failed to look up stakeholder of contracts in domain ${repairSource.domain.alias}: $e"
      )

  private def atLeastOneHostedStakeholderAtTarget(
      contractId: LfContractId,
      stakeholders: Set[LfPartyId],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = {
    EitherT(hostsParties(repairTarget.domain.topologySnapshot, stakeholders, participantId).map {
      hosted =>
        Either.cond(
          hosted.nonEmpty,
          (),
          show"Not allowed to move contract $contractId without at least one stakeholder of $stakeholders existing locally on the target domain asOf=${repairTarget.domain.topologySnapshot.timestamp}",
        )
    })
  }

  private def readContractsFromSource(
      contractIdsWithTransferCounters: List[
        ChangeAssignation.Data[(LfContractId, TransferCounter)]
      ]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[
    (SerializableContract, ChangeAssignation.Data[(LfContractId, TransferCounter)])
  ]] =
    repairSource.domain.persistentState.contractStore
      .lookupManyUncached(contractIdsWithTransferCounters.map(_.payload._1))
      .map(_.map(_.contract).zip(contractIdsWithTransferCounters))
      .leftMap(contractId =>
        s"Failed to look up contract $contractId in domain ${repairSource.domain.alias}"
      )

  private def readContracts(
      contractIdsWithTransferCounters: List[
        ChangeAssignation.Data[(LfContractId, TransferCounter)]
      ]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): EitherT[
    Future,
    String,
    List[ChangeAssignation.Data[Changed]],
  ] =
    readContractsFromSource(contractIdsWithTransferCounters).flatMap {
      _.parTraverse {
        case (
              serializedSource,
              data @ ChangeAssignation.Data((contractId, transferCounter), _, _),
            ) =>
          for {
            transferCounter <- EitherT.fromEither[Future](Right(transferCounter))
            serializedTargetO <- EitherT.right(
              repairTarget.domain.persistentState.contractStore.lookupContract(contractId).value
            )
            _ <- serializedTargetO
              .map { serializedTarget =>
                EitherTUtil.condUnitET[Future](
                  serializedTarget == serializedSource,
                  s"Contract $contractId already exists in the contract store, but differs from contract to be created. Contract to be created $serializedSource versus existing contract $serializedTarget.",
                )
              }
              .getOrElse(EitherT.rightT[Future, String](()))
          } yield data.copy(payload =
            Changed(serializedSource, transferCounter, serializedTargetO.isEmpty)
          )
      }
    }

  private def persistContracts(
      transactionId: TransactionId,
      contracts: List[ChangeAssignation.Data[Changed]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    for {
      _ <- EitherT.right {
        contracts.parTraverse_ { contract =>
          if (contract.payload.isNew)
            repairTarget.domain.persistentState.contractStore
              .storeCreatedContract(
                contract.targetTimeOfChange.rc,
                transactionId,
                contract.payload.contract,
              )
          else Future.unit
        }
      }
    } yield ()

  private def persistTransferOutAndIn(
      contracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): CheckedT[Future, String, ActiveContractStore.AcsWarning, Unit] = {

    val outF = repairSource.domain.persistentState.activeContractStore
      .transferOutContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            targetDomainId,
            contract.payload.transferCounter,
            contract.sourceTimeOfChange,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as transferred out: $e")

    val inF = repairTarget.domain.persistentState.activeContractStore
      .transferInContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            sourceDomainId,
            contract.payload.transferCounter,
            contract.targetTimeOfChange,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as transferred in: $e")

    outF.flatMap(_ => inF)
  }

  private def insertTransferEventsInLog(
      transactionId: TransactionId,
      changedContracts: List[ChangeAssignation.Data[Changed]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = {

    val contracts = changedContracts.map(_.payload.contract)

    val insertTransferOutEvents =
      for {
        hostedParties <- EitherT.right(hostedParties(repairSource, contracts, participantId))
        transferOutEvents = changedContracts.map(transferOut(hostedParties))
        _ <- insertMany(repairSource, transferOutEvents)
      } yield ()

    val insertTransferInEvents =
      for {
        hostedParties <- EitherT.right(hostedParties(repairTarget, contracts, participantId))
        transferInEvents = changedContracts.map(transferIn(transactionId, hostedParties))
        _ <- insertMany(repairTarget, transferInEvents)
      } yield ()

    insertTransferOutEvents.flatMap(_ => insertTransferInEvents)

  }

  private def hostedParties(
      repair: RepairRequest,
      contracts: List[SerializableContract],
      participantId: ParticipantId,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Set[LfPartyId]] = {
    hostsParties(
      repair.domain.topologySnapshot,
      contracts
        .flatMap(_.metadata.stakeholders)
        .toSet,
      participantId,
    )
  }

  private def insertMany(repair: RepairRequest, events: List[TimestampedEvent])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    EitherT(
      repair.domain.persistentState.eventLog.insertsUnlessEventIdClash(events).map(_.sequence)
    )
      .map(_.discard)
      .leftMap { event =>
        show"Unable to insert event with event ID ${event.eventId.showValue} already present at offset ${event.localOffset}"
      }

  private def transferOut(hostedParties: Set[LfPartyId])(
      contract: ChangeAssignation.Data[Changed]
  )(implicit traceContext: TraceContext): TimestampedEvent =
    TimestampedEvent(
      event = LedgerSyncEvent.TransferredOut(
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        optCompletionInfo = None,
        submitter = None,
        contractId = contract.payload.contract.contractId,
        templateId = Option(contract.payload.contract.contractInstance.unversioned.template),
        packageName = contract.payload.contract.contractInstance.unversioned.packageName,
        contractStakeholders = contract.payload.contract.metadata.stakeholders,
        transferId = transferId,
        targetDomain = targetDomainId,
        transferInExclusivity = None,
        workflowId = None,
        isTransferringParticipant = false,
        hostedStakeholders =
          hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
        transferCounter = contract.payload.transferCounter,
      ),
      localOffset = contract.sourceTimeOfChange.asLocalOffset,
      requestSequencerCounter = None,
    )

  private def transferIn(transactionId: TransactionId, hostedParties: Set[LfPartyId])(
      contract: ChangeAssignation.Data[Changed]
  )(implicit traceContext: TraceContext) =
    TimestampedEvent(
      event = LedgerSyncEvent.TransferredIn(
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        optCompletionInfo = None,
        submitter = None,
        recordTime = repairTarget.timestamp.toLf,
        ledgerCreateTime = contract.payload.contract.ledgerCreateTime.toLf,
        createNode = contract.payload.contract.toLf,
        creatingTransactionId = transactionId.tryAsLedgerTransactionId,
        contractMetadata = Bytes.fromByteString(
          contract.payload.contract.metadata
            .toByteString(repairTarget.domain.parameters.protocolVersion)
        ),
        transferId = transferId,
        targetDomain = targetDomainId,
        workflowId = None,
        isTransferringParticipant = false,
        hostedStakeholders =
          hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
        transferCounter = contract.payload.transferCounter,
      ),
      localOffset = contract.targetTimeOfChange.asLocalOffset,
      requestSequencerCounter = None,
    )

}

// TODO(i14540): this needs to be called by RepairService to commit the changes
private[repair] object ChangeAssignation {

  final case class Data[Payload](
      payload: Payload,
      sourceTimeOfChange: TimeOfChange,
      targetTimeOfChange: TimeOfChange,
  )

  /** @param contract Contract that changed its domain
    * @param transferCounter Transfer counter
    * @param isNew true if the contract was not seen before, false if already in the store
    */
  final case class Changed(
      contract: SerializableContract,
      transferCounter: TransferCounter,
      isNew: Boolean,
  )

  def apply(
      contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      participantId: ParticipantId,
      syncCrypto: SyncCryptoApiProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    new ChangeAssignation(
      contractIds,
      repairSource,
      repairTarget,
      skipInactive,
      participantId,
      syncCrypto,
      loggerFactory,
    ).run()

}
