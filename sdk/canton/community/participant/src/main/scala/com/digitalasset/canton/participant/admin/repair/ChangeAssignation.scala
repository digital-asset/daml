// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Functor
import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.ChangeAssignation.Changed
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.ExecutionContext

private final class ChangeAssignation(
    val repairSource: Source[RepairRequest],
    val repairTarget: Target[RepairRequest],
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiProvider,
    repairIndexer: FutureQueue[RepairUpdate],
    contractStore: ContractStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val sourceDomainId = repairSource.map(_.domain.id)
  private val sourceDomainAlias = repairSource.map(_.domain.alias)
  private val sourcePersistentState = repairSource.map(_.domain.persistentState)
  private val targetDomainId = repairTarget.map(_.domain.id)
  private val targetPersistentState = repairTarget.map(_.domain.persistentState)

  /** Completes the processing of unassigned contract. Insert the contract in the target domain
    * and publish the assignment event.
    */
  def completeUnassigned(
      reassignmentData: ChangeAssignation.Data[ReassignmentData]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val contractId = reassignmentData.payload.contract.contractId
    for {
      contractStatusAtSource <- EitherT.right(
        sourcePersistentState.unwrap.activeContractStore
          .fetchStates(Seq(contractId))
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        contractStatusAtSource.get(contractId).exists(_.status.isReassignedAway),
        (),
        s"Contract $contractId is not unassigned in source domain $sourceDomainAlias. " +
          s"Current status: ${contractStatusAtSource.get(contractId).map(_.status.toString).getOrElse("NOT_FOUND")}.",
      )

      unassignedContract <- readContract(
        reassignmentData.map(data => (contractId, data.reassignmentCounter))
      )
      _ <- persistContracts(List(unassignedContract))
      _ <- targetPersistentState.unwrap.reassignmentStore
        .completeReassignment(
          reassignmentData.payload.reassignmentId,
          reassignmentData.targetTimeOfChange.unwrap,
        )
        .toEitherT
      _ <- persistAssignments(List(unassignedContract)).toEitherT
      _ <- EitherT.right(
        publishAssignmentEvent(unassignedContract, reassignmentData.payload.reassignmentId)
      )
    } yield ()
  }

  /** Change the domain assignation for contracts from [[repairSource]] to [[repairTarget]]
    */
  def changeAssignation(
      contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
      skipInactive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      contractStatusAtSource <- EitherT.right(
        sourcePersistentState.unwrap.activeContractStore
          .fetchStates(contractIds.map(_.payload))
      )
      _ = logger.debug(s"Contracts status at source: $contractStatusAtSource")
      contractsAtSource <- changingContractsAtSource(
        contractIds,
        contractStatusAtSource,
        skipInactive,
      )
      sourceContractIds = contractsAtSource.map(_.payload._1)
      _ = logger.debug(s"Contracts changing assignation from source: $sourceContractIds")
      contractStatusAtTarget <- EitherT.right(
        targetPersistentState.unwrap.activeContractStore
          .fetchStates(sourceContractIds)
      )
      _ = logger.debug(s"Contract status at target: $contractStatusAtTarget")
      contractIds <- changingContractIds(contractsAtSource, contractStatusAtTarget)
      _ = logger.debug(s"Contracts changing assignation: $contractIds")
      contracts <- readContracts(contractIds)
      _ = logger.debug(
        s"Contracts that need to change assignation with persistence status: $contracts"
      )
      _ <- persistContracts(contracts)
      _ <- persistUnassignAndAssign(contracts).toEitherT
      _ <- EitherT.right(publishReassignmentEvents(contracts))
    } yield ()

  private def changingContractsAtSource(
      contractIds: Iterable[ChangeAssignation.Data[LfContractId]],
      source: Map[LfContractId, ContractState],
      skipInactive: Boolean,
  ): EitherT[FutureUnlessShutdown, String, List[
    ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
  ]] = {
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
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[
    ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
  ]] = {
    val filteredE =
      sourceContracts
        .traverse { case data @ ChangeAssignation.Data((cid, reassignmentCounter), _, _) =>
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)
          targetStatusOfContract match {
            case None | Some(ActiveContractStore.ReassignedAway(_, _)) =>
              reassignmentCounter.increment
                .map(incrementedTc => data.map { case (cid, _) => (cid, incrementedTc) })
            case Some(targetState) =>
              Left(
                s"Active contract $cid in source domain exists in target domain with status $targetState. Use 'repair.add' or 'repair.purge' instead."
              )
          }
        }

    for {
      filtered <- EitherT.fromEither[FutureUnlessShutdown](filteredE)
      filteredContractIds = filtered.map(_.payload._1)
      stakeholders <- stakeholders(filteredContractIds.toSet)
      _ <- filteredContractIds.parTraverse_ { contractId =>
        atLeastOneHostedStakeholderAtTarget(
          contractId,
          stakeholders.getOrElse(contractId, Set.empty),
        )
      }
    } yield filtered
  }

  private def stakeholders(contractIds: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Set[LfPartyId]]] =
    contractStore
      .lookupStakeholders(contractIds)
      .leftMap(e => s"Failed to look up stakeholder of contracts in domain $sourceDomainAlias: $e")

  private def atLeastOneHostedStakeholderAtTarget(
      contractId: LfContractId,
      stakeholders: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT(
      hostsParties(repairTarget.unwrap.domain.topologySnapshot, stakeholders, participantId).map {
        hosted =>
          Either.cond(
            hosted.nonEmpty,
            (),
            show"Not allowed to move contract $contractId without at least one stakeholder of $stakeholders existing locally on the target domain asOf=${repairTarget.unwrap.domain.topologySnapshot.timestamp}",
          )
      }
    )

  /** Read contract instances from [[ContractStore]]
    */
  private def readContractsInstances(
      contractIdsWithReassignmentCounters: List[
        ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
      ]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[
    (SerializableContract, ChangeAssignation.Data[(LfContractId, ReassignmentCounter)])
  ]] =
    contractStore
      .lookupManyExistingUncached(contractIdsWithReassignmentCounters.map {
        case ChangeAssignation.Data((cid, _), _, _) => cid
      })
      .map(_.map(_.contract).zip(contractIdsWithReassignmentCounters))
      .leftMap(contractId => s"Failed to look up contract $contractId in domain $sourceDomainAlias")
      .mapK(FutureUnlessShutdown.outcomeK)

  private def readContract(
      contractIdWithReassignmentCounter: ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, ChangeAssignation.Data[Changed]] = {
    val contractId = contractIdWithReassignmentCounter.payload._1
    for {
      contracts <- readContracts(List(contractIdWithReassignmentCounter))
      contract <-
        EitherT.fromOption[FutureUnlessShutdown](
          contracts.find(_.payload.contract.contractId == contractId),
          s"Cannot read contract $contractId",
        )
    } yield contract
  }

  private def readContracts(
      contractIdsWithReassignmentCounters: List[
        ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
      ]
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    List[ChangeAssignation.Data[Changed]],
  ] =
    readContractsInstances(contractIdsWithReassignmentCounters).flatMap {
      _.parTraverse {
        case (
              serializableContract,
              data @ ChangeAssignation.Data((contractId, reassignmentCounter), _, _),
            ) =>
          for {
            reassignmentCounter <- EitherT.fromEither[FutureUnlessShutdown](
              Right(reassignmentCounter)
            )
            serializedTargetO <- EitherTUtil.rightUS(
              contractStore.lookupContract(contractId).value
            )
            _ <- serializedTargetO
              .map { serializedTarget =>
                EitherTUtil.condUnitET[FutureUnlessShutdown](
                  serializedTarget == serializableContract,
                  s"Contract $contractId already exists in the contract store, but differs from contract to be created. Contract to be created $serializableContract versus existing contract $serializedTarget.",
                )
              }
              .getOrElse(EitherT.rightT[FutureUnlessShutdown, String](()))
          } yield data.copy(payload =
            Changed(serializableContract, reassignmentCounter, serializedTargetO.isEmpty)
          )
      }
    }

  /** Write contracts in [[ContractStore]]
    */
  private def persistContracts(
      contracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.right {
      contracts.parTraverse_ { contract =>
        if (contract.payload.isNew)
          contractStore
            .storeCreatedContract(
              contract.targetTimeOfChange.unwrap.rc,
              contract.payload.contract,
            )
        else FutureUnlessShutdown.unit
      }
    }

  private def persistAssignments(contracts: List[ChangeAssignation.Data[Changed]])(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] =
    targetPersistentState.unwrap.activeContractStore
      .assignContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            sourceDomainId,
            contract.payload.reassignmentCounter,
            contract.targetTimeOfChange.unwrap,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as assigned: $e")

  private def persistUnassignAndAssign(
      contracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] = {

    val unassignF = sourcePersistentState.unwrap.activeContractStore
      .unassignContracts(
        contracts.map { contract =>
          (
            contract.payload.contract.contractId,
            targetDomainId,
            contract.payload.reassignmentCounter,
            contract.sourceTimeOfChange.unwrap,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as unassigned: $e")

    unassignF
      .flatMap(_ => persistAssignments(contracts))
  }

  private def publishAssignmentEvent(
      changedContract: ChangeAssignation.Data[Changed],
      reassignmentId: ReassignmentId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      hostedTargetParties <- repairTarget
        .traverse(repair =>
          hostedParties(
            repair.domain.topologySnapshot,
            changedContract.payload.contract.metadata.stakeholders,
            participantId,
          )
        )
      _ <- FutureUnlessShutdown.outcomeF(
        repairIndexer.offer(
          assignment(reassignmentId, hostedTargetParties)(traceContext)(changedContract)
        )
      )
    } yield ()

  private def publishReassignmentEvents(
      changedContracts: List[ChangeAssignation.Data[Changed]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val reassignmentId = ReassignmentId(sourceDomainId, repairSource.unwrap.timestamp)
    val allStakeholders = changedContracts.flatMap(_.payload.contract.metadata.stakeholders).toSet

    for {
      hostedSourceParties <- repairSource.traverse(repair =>
        hostedParties(
          repair.domain.topologySnapshot,
          allStakeholders,
          participantId,
        )
      )
      hostedTargetParties <- repairTarget.traverse(repair =>
        hostedParties(
          repair.domain.topologySnapshot,
          allStakeholders,
          participantId,
        )
      )
      _ <- FutureUnlessShutdown.outcomeF(
        MonadUtil.sequentialTraverse_(
          Iterator(
            unassignment(reassignmentId, hostedSourceParties),
            assignment(reassignmentId, hostedTargetParties),
          ).flatMap(changedContracts.map)
        )(repairIndexer.offer)
      )
    } yield ()

  }

  private def hostedParties(
      topologySnapshot: TopologySnapshot,
      stakeholders: Set[LfPartyId],
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[LfPartyId]] =
    hostsParties(
      topologySnapshot,
      stakeholders,
      participantId,
    )

  private def unassignment(
      reassignmentId: ReassignmentId,
      hostedParties: Source[Set[LfPartyId]],
  )(implicit
      traceContext: TraceContext
  ): ChangeAssignation.Data[Changed] => RepairUpdate = contract =>
    Update.RepairReassignmentAccepted(
      workflowId = None,
      updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = sourceDomainId,
        targetDomain = targetDomainId,
        submitter = None,
        reassignmentCounter = contract.payload.reassignmentCounter.v,
        hostedStakeholders =
          hostedParties.unwrap.intersect(contract.payload.contract.metadata.stakeholders).toList,
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
      requestCounter = contract.sourceTimeOfChange.unwrap.rc,
      recordTime = contract.sourceTimeOfChange.unwrap.timestamp,
    )

  private def assignment(
      reassignmentId: ReassignmentId,
      hostedParties: Target[Set[LfPartyId]],
  )(implicit
      traceContext: TraceContext
  ): ChangeAssignation.Data[Changed] => RepairUpdate = contract =>
    Update.RepairReassignmentAccepted(
      workflowId = None,
      updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = sourceDomainId,
        targetDomain = targetDomainId,
        submitter = None,
        reassignmentCounter = contract.payload.reassignmentCounter.v,
        hostedStakeholders =
          hostedParties.unwrap.intersect(contract.payload.contract.metadata.stakeholders).toList,
        unassignId = reassignmentId.unassignmentTs,
        isReassigningParticipant = false,
      ),
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = contract.payload.contract.ledgerCreateTime.toLf,
        createNode = contract.payload.contract.toLf,
        contractMetadata = Bytes.fromByteString(
          contract.payload.contract.metadata
            .toByteString(repairTarget.unwrap.domain.parameters.protocolVersion)
        ),
      ),
      requestCounter = contract.targetTimeOfChange.unwrap.rc,
      recordTime = contract.targetTimeOfChange.unwrap.timestamp,
    )
}

// TODO(i14540): this needs to be called by RepairService to commit the changes
private[repair] object ChangeAssignation {

  final case class Data[Payload](
      payload: Payload,
      sourceTimeOfChange: Source[TimeOfChange],
      targetTimeOfChange: Target[TimeOfChange],
  ) {
    def incrementRequestCounter: Either[String, Data[Payload]] =
      for {
        incrementedSourceRc <- sourceTimeOfChange.unwrap.rc.increment
        incrementedTargetRc <- targetTimeOfChange.unwrap.rc.increment
      } yield copy(
        sourceTimeOfChange = sourceTimeOfChange.map(_.copy(rc = incrementedSourceRc)),
        targetTimeOfChange = targetTimeOfChange.map(_.copy(rc = incrementedTargetRc)),
      )
  }

  object Data {

    implicit val dataFunctorInstance: Functor[Data] = new Functor[Data] {
      // Define the map function for Data
      override def map[A, B](fa: Data[A])(f: A => B): Data[B] =
        Data(f(fa.payload), fa.sourceTimeOfChange, fa.targetTimeOfChange)
    }

    def from[Payload](
        payloads: Seq[Payload],
        changeAssignation: ChangeAssignation,
    ): Either[String, Seq[Data[Payload]]] =
      if (
        payloads.sizeIs == changeAssignation.repairSource.unwrap.timesOfChange.size &&
        payloads.sizeIs == changeAssignation.repairTarget.unwrap.timesOfChange.size
      ) {
        Right(
          payloads
            .zip(changeAssignation.repairSource.traverse(_.timesOfChange))
            .zip(changeAssignation.repairTarget.traverse(_.timesOfChange))
            .map { case ((contractId, sourceToc), targetToc) =>
              ChangeAssignation.Data(contractId, sourceToc, targetToc)
            }
        )
      } else
        Left(
          s"Payloads size ${payloads.size} does not match timesOfChange size ${changeAssignation.repairSource.unwrap.timesOfChange.size} or ${changeAssignation.repairTarget.unwrap.timesOfChange.size}"
        )

    def from[Payload](payload: Payload, changeAssignation: ChangeAssignation): Data[Payload] =
      ChangeAssignation
        .Data(
          payload,
          changeAssignation.repairSource.map(_.firstTimeOfChange),
          changeAssignation.repairTarget.map(_.firstTimeOfChange),
        )
  }

  /** @param contract Contract that changed its domain
    * @param reassignmentCounter Reassignment counter
    * @param isNew true if the contract was not seen before, false if already in the store
    */
  final case class Changed(
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
      isNew: Boolean,
  )
}
