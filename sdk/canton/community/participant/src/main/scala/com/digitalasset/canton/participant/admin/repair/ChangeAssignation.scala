// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Functor
import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.ChangeAssignation.Changed
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData
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

  private val sourceSynchronizerId = repairSource.map(_.synchronizer.id)
  private val sourceSynchronizerAlias = repairSource.map(_.synchronizer.alias)
  private val sourcePersistentState = repairSource.map(_.synchronizer.persistentState)
  private val targetSynchronizerId = repairTarget.map(_.synchronizer.id)
  private val targetPersistentState = repairTarget.map(_.synchronizer.persistentState)

  /** Completes the processing of unassigned contract. Insert the contract in the target synchronizer
    * and publish the assignment event.
    */
  def completeUnassigned(
      unassignmentData: ChangeAssignation.Data[UnassignmentData]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val contractId = unassignmentData.payload.contract.contractId
    for {
      contractStatusAtSource <- EitherT.right(
        sourcePersistentState.unwrap.activeContractStore
          .fetchStates(Seq(contractId))
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        contractStatusAtSource.get(contractId).exists(_.status.isReassignedAway),
        (),
        s"Contract $contractId is not unassigned in source synchronizer $sourceSynchronizerAlias. " +
          s"Current status: ${contractStatusAtSource.get(contractId).map(_.status.toString).getOrElse("NOT_FOUND")}.",
      )
      unassignedContract <- readContract(contractId)
      _ <- persistContracts(List(unassignedContract))
      _ <- targetPersistentState.unwrap.reassignmentStore
        .completeReassignment(
          unassignmentData.payload.reassignmentId,
          unassignmentData.targetTimeOfChange.unwrap,
        )
        .toEitherT
      _ <- persistAssignments(
        List(
          (
            contractId,
            unassignmentData.targetTimeOfChange,
            unassignmentData.payload.reassignmentCounter,
          )
        )
      ).toEitherT

      _ <- EitherT.right(
        publishAssignmentEvent(
          unassignmentData.map(_ => unassignedContract),
          unassignmentData.payload.reassignmentId,
          unassignmentData.payload.reassignmentCounter,
        )
      )
    } yield ()
  }

  /** Change the synchronizer assignation for contracts from [[repairSource]] to [[repairTarget]]
    */
  def changeAssignation(
      contracts: Iterable[ChangeAssignation.Data[(LfContractId, Option[ReassignmentCounter])]],
      skipInactive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    val reassignmentCounterOverride: Map[LfContractId, ReassignmentCounter] =
      contracts.toSeq.mapFilter { case ChangeAssignation.Data((cid, reassignmentCounterO), _, _) =>
        reassignmentCounterO.map((cid, _))
      }.toMap

    for {
      filteredContractsIds <- filterContracts(
        contracts.map(_.map { case (cid, _) => cid }),
        skipInactive = skipInactive,
      ).flatMap { filteredContracts =>
        EitherT.fromEither[FutureUnlessShutdown](filteredContracts.traverse { data =>
          val (cid, reassignmentCounter) = data.payload
          val newReassignmentCounterE =
            reassignmentCounterOverride.get(cid).fold(reassignmentCounter.increment)(Right(_))

          newReassignmentCounterE.map(newReassignmentCounter =>
            data.map(_ => (cid, newReassignmentCounter))
          )
        })
      }

      _ = logger.debug(s"Contracts changing assignation: $filteredContractsIds")
      filteredChanges <- readContracts(filteredContractsIds.map(_.payload._1))
      _ = logger.debug(
        s"Contracts that need to change assignation with persistence status: $filteredChanges"
      )
      _ <- persistContracts(filteredChanges)
      filteredContracts = filteredContractsIds.zip(filteredChanges).map { case (data, change) =>
        val (_, reassignmentCounter) = data.payload
        (data.map(_ => change), reassignmentCounter)
      }
      _ <- persistUnassignAndAssign(filteredContracts).toEitherT
      _ <- EitherT.right(publishReassignmentEvents(filteredContracts))
    } yield ()
  }

  /** Filter out contracts whose assignation should not be changed.
    * Checks status on source and target synchronizer.
    * @return Data is enriched with reassignment counter.
    */
  private def filterContracts(
      contracts: Iterable[ChangeAssignation.Data[LfContractId]],
      skipInactive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[
    ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
  ]] = for {
    contractStatusAtSource <- EitherT.right(
      sourcePersistentState.unwrap.activeContractStore
        .fetchStates(contracts.map(_.payload))
    )
    _ = logger.debug(s"Contracts status at source: $contractStatusAtSource")
    contractsAtSource <- filterContractsAtSource(
      contracts,
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

    filteredContractsIds <- filterContractsAtTarget(contractsAtSource, contractStatusAtTarget)
  } yield filteredContractsIds

  /** Keep only contracts which have proper state in the source ActiveContractStore.
    * Enrich the data with reassignment counter.
    */
  private def filterContractsAtSource(
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
            errorUnlessSkipInactive(cid, "does not exist in source synchronizer")
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

  /** Keep only contracts which satisfy:
    *
    * - At least one stakeholder is hosted on the target synchronizer
    * - State of the contract on the target synchronizer is not active or archived
    */
  private def filterContractsAtTarget(
      sourceContracts: List[ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]],
      targetStatus: Map[LfContractId, ContractState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[
    ChangeAssignation.Data[(LfContractId, ReassignmentCounter)]
  ]] = {
    val filteredE =
      sourceContracts
        .traverse { data =>
          val cid = data.payload._1
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)

          targetStatusOfContract match {

            // Contract is inactive on the target synchronizer
            case None | Some(_: ActiveContractStore.ReassignedAway) |
                Some(ActiveContractStore.Purged) =>
              Right(data)

            case Some(targetState) =>
              Left(
                s"Active contract $cid in source synchronizer exists in target synchronizer with status $targetState. Use 'repair.add' or 'repair.purge' instead."
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
      .leftMap(e =>
        s"Failed to look up stakeholder of contracts in synchronizer $sourceSynchronizerAlias: $e"
      )

  private def atLeastOneHostedStakeholderAtTarget(
      contractId: LfContractId,
      stakeholders: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT(
      hostsParties(repairTarget.unwrap.synchronizer.topologySnapshot, stakeholders, participantId)
        .map { hosted =>
          Either.cond(
            hosted.nonEmpty,
            (),
            show"Not allowed to change assignation of contract $contractId without at least one stakeholder of $stakeholders existing locally on the target synchronizer asOf=${repairTarget.unwrap.synchronizer.topologySnapshot.timestamp}",
          )
        }
    )

  /** Read contract instances from [[ContractStore]]
    */
  private def readContractsInstances(
      contractIds: List[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[SerializableContract]] =
    contractStore
      .lookupManyExistingUncached(contractIds)
      .leftMap(contractId =>
        s"Failed to look up contract $contractId in synchronizer $sourceSynchronizerAlias"
      )

  private def readContract(contractId: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Changed] =
    for {
      contracts <- readContracts(List(contractId))
      contract <-
        EitherT.fromOption[FutureUnlessShutdown](
          contracts.find(_.contract.contractId == contractId),
          s"Cannot read contract $contractId",
        )
    } yield contract

  private def readContracts(
      contractIds: List[LfContractId]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, List[Changed]] =
    readContractsInstances(contractIds).flatMap {
      _.parTraverse { serializableContract =>
        val contractId = serializableContract.contractId

        for {
          serializedTargetO <- EitherT.right(contractStore.lookupContract(contractId).value)
          _ <- serializedTargetO
            .map { serializedTarget =>
              EitherTUtil.condUnitET[FutureUnlessShutdown](
                serializedTarget == serializableContract,
                s"Contract $contractId already exists in the contract store, but differs from contract to be created. Contract to be created $serializableContract versus existing contract $serializedTarget.",
              )
            }
            .getOrElse(EitherT.rightT[FutureUnlessShutdown, String](()))
        } yield Changed(serializableContract, serializedTargetO.isEmpty)
      }
    }

  /** Write contracts in [[ContractStore]]
    */
  private def persistContracts(contracts: List[Changed])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.right {
      contracts.parTraverse_ { contract =>
        if (contract.isNew)
          contractStore.storeContract(contract.contract)
        else FutureUnlessShutdown.unit
      }
    }

  private def persistAssignments(
      contracts: List[(LfContractId, Target[TimeOfChange], ReassignmentCounter)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] =
    targetPersistentState.unwrap.activeContractStore
      .assignContracts(
        contracts.map { case (cid, toc, reassignmentCounter) =>
          (
            cid,
            sourceSynchronizerId,
            reassignmentCounter,
            toc.unwrap,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as assigned: $e")

  private def persistUnassignAndAssign(
      contracts: List[(ChangeAssignation.Data[Changed], ReassignmentCounter)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] = {

    val unassignF = sourcePersistentState.unwrap.activeContractStore
      .unassignContracts(
        contracts.map { case (contract, reassignmentCounter) =>
          (
            contract.payload.contract.contractId,
            targetSynchronizerId,
            reassignmentCounter,
            contract.sourceTimeOfChange.unwrap,
          )
        }
      )
      .mapAbort(e => s"Failed to mark contracts as unassigned: $e")

    unassignF
      .flatMap(_ =>
        persistAssignments(contracts.map { case (data, reassignmentCounter) =>
          (data.payload.contract.contractId, data.targetTimeOfChange, reassignmentCounter)
        })
      )
  }

  private def publishAssignmentEvent(
      changedContract: ChangeAssignation.Data[Changed],
      reassignmentId: ReassignmentId,
      reassignmentCounter: ReassignmentCounter,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      hostedTargetParties <- repairTarget
        .traverse(repair =>
          hostedParties(
            repair.synchronizer.topologySnapshot,
            changedContract.payload.contract.metadata.stakeholders,
            participantId,
          )
        )
      _ <- FutureUnlessShutdown.outcomeF(
        repairIndexer.offer(
          assignment(reassignmentId, hostedTargetParties)(traceContext)(
            changedContract,
            reassignmentCounter,
          )
        )
      )
    } yield ()

  private def publishReassignmentEvents(
      changedContracts: List[(ChangeAssignation.Data[Changed], ReassignmentCounter)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val reassignmentId = ReassignmentId(sourceSynchronizerId, repairSource.unwrap.timestamp)
    val allStakeholders = changedContracts.flatMap { case (contract, _) =>
      contract.payload.contract.metadata.stakeholders
    }.toSet

    for {
      hostedSourceParties <- repairSource.traverse(repair =>
        hostedParties(
          repair.synchronizer.topologySnapshot,
          allStakeholders,
          participantId,
        )
      )
      hostedTargetParties <- repairTarget.traverse(repair =>
        hostedParties(
          repair.synchronizer.topologySnapshot,
          allStakeholders,
          participantId,
        )
      )
      _ <- FutureUnlessShutdown.outcomeF(
        MonadUtil.sequentialTraverse_(
          Iterator(
            unassignment(reassignmentId, hostedSourceParties) tupled,
            assignment(reassignmentId, hostedTargetParties) tupled,
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
  ): (ChangeAssignation.Data[Changed], ReassignmentCounter) => RepairUpdate =
    (contract, reassignmentCounter) =>
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourceSynchronizerId,
          targetSynchronizer = targetSynchronizerId,
          submitter = None,
          reassignmentCounter = reassignmentCounter.v,
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
  ): (ChangeAssignation.Data[Changed], ReassignmentCounter) => RepairUpdate =
    (contract, reassignmentCounter) =>
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourceSynchronizerId,
          targetSynchronizer = targetSynchronizerId,
          submitter = None,
          reassignmentCounter = reassignmentCounter.v,
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
              .toByteString(repairTarget.unwrap.synchronizer.parameters.protocolVersion)
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

    def map[T](f: Payload => T): Data[T] = this.copy(payload = f(payload))
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
        payloads.sizeCompare(changeAssignation.repairSource.unwrap.timesOfChange) == 0 &&
        payloads.sizeCompare(changeAssignation.repairTarget.unwrap.timesOfChange) == 0
      ) {
        Right(
          payloads
            .zip(changeAssignation.repairSource.traverse(_.timesOfChange))
            .zip(changeAssignation.repairTarget.traverse(_.timesOfChange))
            .map { case ((data, sourceToc), targetToc) =>
              ChangeAssignation.Data(data, sourceToc, targetToc)
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

  /** @param contract Contract that is reassigned
    * @param isNew true if the contract was not seen before, false if already in the store
    */
  final case class Changed(
      contract: SerializableContract,
      isNew: Boolean,
  )
}
