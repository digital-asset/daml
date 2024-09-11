// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.ledger.participant.state.{
  DomainIndex,
  Reassignment,
  ReassignmentInfo,
  RequestIndex,
  Update,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.ChangeAssignation.Changed
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.{ExecutionContext, Future}

private final class ChangeAssignation(
    contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
    repairSource: RepairRequest,
    repairTarget: RepairRequest,
    skipInactive: Boolean,
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiProvider,
    repairIndexer: FutureQueue[Traced[Update]],
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val sourceDomainId = SourceDomainId(repairSource.domain.id)
  private val targetDomainId = TargetDomainId(repairTarget.domain.id)
  private val reassignmentId = ReassignmentId(sourceDomainId, repairSource.timestamp)

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
      _ <- persistUnassignAndAssign(contracts).toEitherT
      _ <- EitherT.right(insertReassignmentEventsInLog(contracts))
    } yield ()

  private def changingContractsAtSource(
      source: Map[LfContractId, ContractState]
  )(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, String, List[ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]]] = {
    def errorUnlessSkipInactive(
        cid: ChangeAssignation.Data[LfContractId],
        reason: String,
    ): Either[String, None.type] =
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
          case (cid, Some(ActiveContractStore.Active(reassignmentCounter))) =>
            Right(Some(cid.copy(payload = (cid.payload, reassignmentCounter))))
          case (cid, Some(ActiveContractStore.Archived)) =>
            errorUnlessSkipInactive(cid, "has been archived")
          case (cid, Some(ActiveContractStore.Purged)) =>
            errorUnlessSkipInactive(cid, "has been purged")
          case (cid, Some(ActiveContractStore.ReassignedAway(target, _reassignmentCounter))) =>
            errorUnlessSkipInactive(cid, s"has been reassigned to $target")
        }
        .map(_.flatten)
    )
  }

  private def changingContractIds(
      sourceContracts: List[ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]],
      targetStatus: Map[LfContractId, ContractState],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]]] = {
    val filteredE =
      sourceContracts
        .traverse { case data @ ChangeAssignation.Data((cid, reassignmentCounter), _, _) =>
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)
          targetStatusOfContract match {
            case None | Some(ActiveContractStore.ReassignedAway(_, _)) =>
              reassignmentCounter.increment
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
  ): EitherT[Future, String, Unit] =
    EitherT(hostsParties(repairTarget.domain.topologySnapshot, stakeholders, participantId).map {
      hosted =>
        Either.cond(
          hosted.nonEmpty,
          (),
          show"Not allowed to move contract $contractId without at least one stakeholder of $stakeholders existing locally on the target domain asOf=${repairTarget.domain.topologySnapshot.timestamp}",
        )
    })

  private def readContractsFromSource(
      contractIdsWithReassignmentCounters: List[
        ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
      ]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, List[
    (SerializableContract, ChangeAssignation.Data[(LfContractId, ReassignmentCounter)])
  ]] =
    repairSource.domain.persistentState.contractStore
      .lookupManyExistingUncached(contractIdsWithReassignmentCounters.map(_.payload._1))
      .map(_.map(_.contract).zip(contractIdsWithReassignmentCounters))
      .leftMap(contractId =>
        s"Failed to look up contract $contractId in domain ${repairSource.domain.alias}"
      )

  private def readContracts(
      contractIdsWithReassignmentCounters: List[
        ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
      ]
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): EitherT[
    Future,
    String,
    List[ChangeAssignation.Data[Changed]],
  ] =
    readContractsFromSource(contractIdsWithReassignmentCounters).flatMap {
      _.parTraverse {
        case (
              serializedSource,
              data @ ChangeAssignation.Data((contractId, reassignmentCounter), _, _),
            ) =>
          for {
            reassignmentCounter <- EitherT.fromEither[Future](Right(reassignmentCounter))
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
            Changed(serializedSource, reassignmentCounter, serializedTargetO.isEmpty)
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

  private def persistUnassignAndAssign(
      contracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): CheckedT[Future, String, ActiveContractStore.AcsWarning, Unit] = {

    val unassignF = repairSource.domain.persistentState.activeContractStore
      .unassignContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            targetDomainId,
            contract.payload.reassignmentCounter,
            contract.sourceTimeOfChange,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as unassigned: $e")

    val assignF = repairTarget.domain.persistentState.activeContractStore
      .assignContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            sourceDomainId,
            contract.payload.reassignmentCounter,
            contract.targetTimeOfChange,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as assigned: $e")

    unassignF.flatMap(_ => assignF)
  }

  private def insertReassignmentEventsInLog(
      changedContracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] =
    for {
      hostedSourceParties <- hostedParties(repairSource, changedContracts, participantId)
      hostedTargetParties <- hostedParties(repairTarget, changedContracts, participantId)
      _ <- MonadUtil.sequentialTraverse_(
        Iterator(
          unassignment(hostedSourceParties),
          assignment(hostedTargetParties),
        ).flatMap(changedContracts.map)
      )(repairIndexer.offer)
    } yield ()

  private def hostedParties(
      repair: RepairRequest,
      changedContracts: List[ChangeAssignation.Data[Changed]],
      participantId: ParticipantId,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Set[LfPartyId]] =
    hostsParties(
      repair.domain.topologySnapshot,
      changedContracts
        .flatMap(_.payload.contract.metadata.stakeholders)
        .toSet,
      participantId,
    )

  private def unassignment(hostedParties: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): ChangeAssignation.Data[Changed] => Traced[Update] = contract =>
    Traced(
      Update.ReassignmentAccepted(
        optCompletionInfo = None,
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        recordTime = reassignmentId.unassignmentTs.underlying,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = reassignmentId.sourceDomain,
          targetDomain = targetDomainId,
          submitter = None,
          reassignmentCounter = contract.payload.reassignmentCounter.v,
          hostedStakeholders =
            hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
          unassignId = reassignmentId.unassignmentTs,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Unassign(
          contractId = contract.payload.contract.contractId,
          templateId = contract.payload.contract.contractInstance.unversioned.template,
          packageName = contract.payload.contract.contractInstance.unversioned.packageName,
          stakeholders = contract.payload.contract.metadata.stakeholders.toList,
          assignmentExclusivity = None,
        ),
        domainIndex = Some(
          DomainIndex.of(
            RequestIndex(
              counter = contract.sourceTimeOfChange.rc,
              sequencerCounter = None,
              timestamp = contract.sourceTimeOfChange.timestamp,
            )
          )
        ),
      )
    )

  private def assignment(hostedParties: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): ChangeAssignation.Data[Changed] => Traced[Update] = contract =>
    Traced(
      Update.ReassignmentAccepted(
        optCompletionInfo = None,
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        recordTime = repairTarget.timestamp.toLf,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = reassignmentId.sourceDomain,
          targetDomain = targetDomainId,
          submitter = None,
          reassignmentCounter = contract.payload.reassignmentCounter.v,
          hostedStakeholders =
            hostedParties.intersect(contract.payload.contract.metadata.stakeholders).toList,
          unassignId = reassignmentId.unassignmentTs,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Assign(
          ledgerEffectiveTime = contract.payload.contract.ledgerCreateTime.toLf,
          createNode = contract.payload.contract.toLf,
          contractMetadata = Bytes.fromByteString(
            contract.payload.contract.metadata
              .toByteString(repairTarget.domain.parameters.protocolVersion)
          ),
        ),
        domainIndex = Some(
          DomainIndex.of(
            RequestIndex(
              counter = contract.targetTimeOfChange.rc,
              sequencerCounter = None,
              timestamp = contract.targetTimeOfChange.timestamp,
            )
          )
        ),
      )
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
    * @param reassignmentCounter Reassignment counter
    * @param isNew true if the contract was not seen before, false if already in the store
    */
  final case class Changed(
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
      isNew: Boolean,
  )

  def apply(
      contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      participantId: ParticipantId,
      syncCrypto: SyncCryptoApiProvider,
      repairIndexer: FutureQueue[Traced[Update]],
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
      repairIndexer,
      loggerFactory,
    ).run()

}
