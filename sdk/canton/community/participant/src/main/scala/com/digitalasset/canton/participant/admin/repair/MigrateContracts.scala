// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.data.{EitherT, OptionT}
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.data.Bytes
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.MigrateContracts.MigratedContract
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

private final class MigrateContracts(
    contractIds: Iterable[MigrateContracts.Data[LfContractId]],
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

  /** Migrate contracts from [[repairSource]] to [[repairTarget]]
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
      sourceContractsToMigrate <- determineSourceContractsToMigrate(contractStatusAtSource)
      sourceContractIdsToMigrate = sourceContractsToMigrate.map(_.payload)
      _ = logger.debug(s"Contracts to migrate from source: $sourceContractIdsToMigrate")
      contractStatusAtTarget <- EitherT.right(
        repairTarget.domain.persistentState.activeContractStore
          .fetchStates(sourceContractIdsToMigrate)
      )
      _ = logger.debug(s"Contract status at target: $contractStatusAtTarget")
      contractIds <- determineTargetContractsToMigrate(
        sourceContractsToMigrate,
        contractStatusAtTarget,
      )
      _ = logger.debug(s"Contracts to migrate: $contractIds")
      contracts <- readContracts(contractIds)
      _ = logger.debug(s"Contracts that need to be migrated with persistence status: $contracts")
      transactionId = randomTransactionId(syncCrypto)
      _ <- persistContracts(transactionId, contracts)
      _ <- persistTransferOutAndIn(contracts).toEitherT
      _ <- insertTransferEventsInLog(transactionId, contracts)
    } yield ()

  private def determineSourceContractsToMigrate(
      source: Map[LfContractId, ContractState]
  )(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, String, List[MigrateContracts.Data[LfContractId]]] = {
    def errorUnlessSkipInactive(cid: MigrateContracts.Data[LfContractId], reason: String) =
      Either.cond(
        skipInactive,
        None,
        s"Cannot change contract assignation: contract $cid $reason.",
      )

    EitherT.fromEither(
      contractIds
        .map(cid => (cid, source.get(cid.payload).map(_.status)))
        .toList
        .traverse {
          case (cid, None) =>
            errorUnlessSkipInactive(cid, "does not exist in source domain")
          case (cid, Some(ActiveContractStore.Active)) =>
            Right(Some(cid.copy(payload = cid.payload)))
          case (cid, Some(ActiveContractStore.Archived)) =>
            errorUnlessSkipInactive(cid, "has been archived")
          case (cid, Some(ActiveContractStore.Purged)) =>
            errorUnlessSkipInactive(cid, "has been purged")
          case (cid, Some(ActiveContractStore.TransferredAway(target))) =>
            errorUnlessSkipInactive(cid, s"has been transferred to $target")
        }
        .map(_.flatten)
    )
  }

  private def determineTargetContractsToMigrate(
      sourceContracts: List[MigrateContracts.Data[LfContractId]],
      targetStatus: Map[LfContractId, ContractState],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[MigrateContracts.Data[LfContractId]]] = {
    val filteredE =
      sourceContracts
        .traverse { case data @ MigrateContracts.Data(cid, _, _) =>
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)
          targetStatusOfContract match {
            case None | Some(ActiveContractStore.TransferredAway(_)) =>
              Right(data)
            case Some(targetState) =>
              Left(
                s"Active contract $cid in source domain exists in target domain with status $targetState. Use 'repair.add' or 'repair.purge' instead."
              )
          }
        }

    for {
      filtered <- EitherT.fromEither[Future](filteredE)
      filteredContractIds = filtered.map(_.payload)
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
  )(implicit executionContext: ExecutionContext): EitherT[Future, String, Unit] =
    OptionT(
      stakeholders.toSeq
        .findM(hostsParty(repairTarget.domain.topologySnapshot, participantId))
    ).map(_.discard)
      .toRight(
        show"Not allowed to move contract $contractId without at least one stakeholder of $stakeholders existing locally on the target domain asOf=${repairTarget.domain.topologySnapshot.timestamp}"
      )

  private def readContractsFromSource(
      cids: List[MigrateContracts.Data[LfContractId]]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[
    (SerializableContract, MigrateContracts.Data[LfContractId])
  ]] =
    repairSource.domain.persistentState.contractStore
      .lookupManyUncached(cids.map(_.payload))
      .map(_.map(_.contract).zip(cids))
      .leftMap(contractId =>
        s"Failed to look up contract $contractId in domain ${repairSource.domain.alias}"
      )

  private def readContracts(
      contractIds: List[MigrateContracts.Data[LfContractId]]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): EitherT[
    Future,
    String,
    List[MigrateContracts.Data[MigratedContract]],
  ] =
    readContractsFromSource(contractIds).flatMap {
      _.parTraverse {
        case (
              serializedSource,
              data @ MigrateContracts.Data(contractId, _, _),
            ) =>
          for {
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
          } yield data.copy(payload = MigratedContract(serializedSource, serializedTargetO.isEmpty))
      }
    }

  private def adjustContractKeys(
      request: RepairRequest,
      timeOfChange: MigrateContracts.Data[MigratedContract] => TimeOfChange,
      contracts: List[MigrateContracts.Data[MigratedContract]],
      newStatus: ContractKeyJournal.Status,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    if (!request.domain.parameters.uniqueContractKeys)
      EitherT.rightT(())
    else
      for {
        keys <- EitherT.right(
          contracts.parTraverseFilter(contract =>
            getKeyIfOneMaintainerIsLocal(
              request.domain.topologySnapshot,
              participantId,
              hostedPartiesO = None,
              contract.payload.contract.metadata.maybeKeyWithMaintainers,
            ).map(_.map(_ -> timeOfChange(contract)))
          )
        )
        _ <- request.domain.persistentState.contractKeyJournal
          .addKeyStateUpdates(keys.map { case (key, toc) => key -> (newStatus, toc) }.toMap)
          .leftMap(_.toString)
      } yield ()

  private def persistContracts(
      transactionId: TransactionId,
      contracts: List[MigrateContracts.Data[MigratedContract]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    for {
      _ <- adjustContractKeys(
        repairSource,
        _.sourceTimeOfChange,
        contracts,
        ContractKeyJournal.Unassigned,
      )
      _ <- adjustContractKeys(
        repairTarget,
        _.targetTimeOfChange,
        contracts,
        ContractKeyJournal.Assigned,
      )
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
      contracts: List[MigrateContracts.Data[MigratedContract]]
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
            contract.targetTimeOfChange,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as transferred in: $e")

    outF.flatMap(_ => inF)
  }

  private def insertTransferEventsInLog(
      transactionId: TransactionId,
      migratedContracs: List[MigrateContracts.Data[MigratedContract]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = {

    val contracts = migratedContracs.map(_.payload.contract)

    val insertTransferOutEvents =
      for {
        hostedParties <- EitherT.right(hostedParties(repairSource, contracts, participantId))
        transferOutEvents = migratedContracs.map(transferOut(hostedParties))
        _ <- insertMany(repairSource, transferOutEvents)
      } yield ()

    val insertTransferInEvents =
      for {
        hostedParties <- EitherT.right(hostedParties(repairTarget, contracts, participantId))
        transferInEvents = migratedContracs.map(transferIn(transactionId, hostedParties))
        _ <- insertMany(repairTarget, transferInEvents)
      } yield ()

    insertTransferOutEvents.flatMap(_ => insertTransferInEvents)

  }

  private def hostedParties(
      repair: RepairRequest,
      contracts: List[SerializableContract],
      participantId: ParticipantId,
  )(implicit executionContext: ExecutionContext): Future[Set[LfPartyId]] =
    contracts
      .flatMap(_.metadata.stakeholders)
      .distinct
      .parTraverseFilter(party =>
        hostsParty(repair.domain.topologySnapshot, participantId)(party).map(Option.when(_)(party))
      )
      .map(_.toSet)

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
      contract: MigrateContracts.Data[MigratedContract]
  )(implicit traceContext: TraceContext): TimestampedEvent =
    TimestampedEvent(
      event = LedgerSyncEvent.TransferredOut(
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        optCompletionInfo = None,
        submitter = None,
        contractId = contract.payload.contract.contractId,
        templateId = Option(contract.payload.contract.contractInstance.unversioned.template),
        contractStakeholders = contract.payload.contract.metadata.stakeholders,
        transferId = transferId,
        targetDomain = targetDomainId,
        transferInExclusivity = None,
        workflowId = None,
        isTransferringParticipant = false,
        hostedStakeholders =
          hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
      ),
      localOffset = contract.sourceTimeOfChange.asLocalOffset,
      requestSequencerCounter = None,
    )

  private def transferIn(transactionId: TransactionId, hostedParties: Set[LfPartyId])(
      contract: MigrateContracts.Data[MigratedContract]
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
        createTransactionAccepted = false,
        workflowId = None,
        isTransferringParticipant = false,
        hostedStakeholders =
          hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
      ),
      localOffset = contract.targetTimeOfChange.asLocalOffset,
      requestSequencerCounter = None,
    )

}

// TODO(i14540): this needs to be called by RepairService to commit the changes
private[repair] object MigrateContracts {

  final case class Data[Payload](
      payload: Payload,
      sourceTimeOfChange: TimeOfChange,
      targetTimeOfChange: TimeOfChange,
  )

  /** @param contract Contract to be migrated
    * @param isNew true if the contract was not seen before, false if already in the store
    */
  final case class MigratedContract(
      contract: SerializableContract,
      isNew: Boolean,
  )

  def apply(
      contractIds: Iterable[MigrateContracts.Data[LfContractId]],
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
    new MigrateContracts(
      contractIds,
      repairSource,
      repairTarget,
      skipInactive,
      participantId,
      syncCrypto,
      loggerFactory,
    ).run()

}
