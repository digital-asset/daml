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
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.ContractsReassignmentBatch
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.ChangeAssignation.Changes
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.ParticipantId
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
    syncCrypto: SyncCryptoApiParticipantProvider,
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

  import com.digitalasset.canton.util.MonadUtil

  /** Completes the processing of unassigned contract. Insert the contract in the target
    * synchronizer and publish the assignment event.
    */
  def completeUnassigned(
      unassignmentData: ChangeAssignation.Data[UnassignmentData]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val contractIdCounters = unassignmentData.payload.contracts.contractIdCounters.toSeq
    val contractIds = contractIdCounters.map(_._1)

    for {
      contractStatusAtSource <- EitherT.right(
        sourcePersistentState.unwrap.activeContractStore.fetchStates(contractIds)
      )
      _ <- MonadUtil.sequentialTraverse(contractIds) { contractId =>
        EitherT.cond[FutureUnlessShutdown](
          contractStatusAtSource.get(contractId).exists(_.status.isReassignedAway),
          (),
          s"Contract $contractId is not unassigned in source synchronizer $sourceSynchronizerAlias. " +
            s"Current status: ${contractStatusAtSource.get(contractId).map(_.status.toString).getOrElse("NOT_FOUND")}.",
        )
      }
      unassignedContracts <- readContracts(contractIdCounters)
      _ <- persistContracts(unassignedContracts)
      _ <- targetPersistentState.unwrap.reassignmentStore
        .completeReassignment(
          unassignmentData.payload.reassignmentId,
          unassignmentData.targetTimeOfRepair.unwrap.timestamp,
        )
        .toEitherT

      _ <- persistAssignments(
        unassignmentData.payload.contracts.contractIdCounters.map {
          case (contractId, reassignmentCounter) =>
            (contractId, unassignmentData.targetTimeOfRepair, reassignmentCounter)
        }
      ).toEitherT

      _ <- EitherT.right(
        publishAssignmentEvent(
          unassignmentData.payload.reassignmentId,
          unassignmentData.map(_ => unassignedContracts),
        )
      )
    } yield ()
  }

  /** Change the synchronizer assignation for contracts from [[repairSource]] to [[repairTarget]]
    */
  def changeAssignation(
      changes: ChangeAssignation.Data[Seq[(LfContractId, Option[ReassignmentCounter])]],
      skipInactive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    val reassignmentCounterOverride: Map[LfContractId, ReassignmentCounter] =
      changes.payload.mapFilter { case (cid, reassignmentCounterO) =>
        reassignmentCounterO.map((cid, _))
      }.toMap

    for {
      filteredContractsIds <- filterContracts(
        changes.payload.map { case (cid, _) => cid },
        skipInactive = skipInactive,
      ).flatMap { filteredContracts =>
        EitherT.fromEither[FutureUnlessShutdown](filteredContracts.traverse {
          case (cid, reassignmentCounter) =>
            val newReassignmentCounterE =
              reassignmentCounterOverride.get(cid).fold(reassignmentCounter.increment)(Right(_))

            newReassignmentCounterE.map(newReassignmentCounter => (cid, newReassignmentCounter))
        })
      }

      _ = logger.debug(s"Contracts changing assignation: $filteredContractsIds")
      changeBatch <- readContracts(filteredContractsIds)
      _ = logger.debug(
        s"Contracts that need to change assignation with persistence status: $changeBatch"
      )
      _ <- persistContracts(changeBatch)
      newChanges = changes.map(_ => changeBatch)
      _ <- persistUnassignAndAssign(newChanges).toEitherT
      _ <- EitherT.right(publishReassignmentEvents(newChanges))
    } yield ()
  }

  /** Filter out contracts whose assignation should not be changed. Checks status on source and
    * target synchronizer.
    * @return
    *   Data is enriched with reassignment counter.
    */
  private def filterContracts(
      contracts: Iterable[LfContractId],
      skipInactive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Seq[(LfContractId, ReassignmentCounter)]] = for {
    contractStatusAtSource <- EitherT.right(
      sourcePersistentState.unwrap.activeContractStore.fetchStates(contracts)
    )
    _ = logger.debug(s"Contracts status at source: $contractStatusAtSource")
    sourceContractIds <- filterContractsAtSource(
      contracts,
      contractStatusAtSource,
      skipInactive,
    )
    _ = logger.debug(s"Contracts changing assignation from source: $sourceContractIds")
    contractStatusAtTarget <- EitherT.right(
      targetPersistentState.unwrap.activeContractStore
        .fetchStates(sourceContractIds.map(_._1))
    )
    _ = logger.debug(s"Contract status at target: $contractStatusAtTarget")

    filteredContractsIds <- filterContractsAtTarget(sourceContractIds, contractStatusAtTarget)
  } yield filteredContractsIds

  /** Keep only contracts which have proper state in the source ActiveContractStore. Enrich the data
    * with reassignment counter.
    */
  private def filterContractsAtSource(
      contractIds: Iterable[LfContractId],
      source: Map[LfContractId, ContractState],
      skipInactive: Boolean,
  ): EitherT[FutureUnlessShutdown, String, Seq[(LfContractId, ReassignmentCounter)]] = {
    def errorUnlessSkipInactive(
        cid: LfContractId,
        reason: String,
    ): Either[String, None.type] =
      Either.cond(
        skipInactive,
        None,
        s"Cannot change contract assignation: contract $cid $reason.",
      )

    EitherT.fromEither(
      contractIds
        .map(cid => (cid, source.get(cid).map(_.status)))
        .toSeq
        .traverse {
          case (cid, None) =>
            errorUnlessSkipInactive(cid, "does not exist in source synchronizer")
          case (cid, Some(ActiveContractStore.Archived)) =>
            errorUnlessSkipInactive(cid, "has been archived")
          case (cid, Some(ActiveContractStore.Purged)) =>
            errorUnlessSkipInactive(cid, "has been purged")
          case (cid, Some(ActiveContractStore.ReassignedAway(target, _reassignmentCounter))) =>
            errorUnlessSkipInactive(cid, s"has been reassigned to $target")
          case (cid, Some(ActiveContractStore.Active(reassignmentCounter))) =>
            Right(Some((cid, reassignmentCounter)))
        }
        .map(_.flatten)
    )
  }

  /** Keep only contracts which satisfy:
    *
    *   - At least one stakeholder is hosted on the target synchronizer
    *   - State of the contract on the target synchronizer is not active or archived
    */
  private def filterContractsAtTarget(
      sourceContracts: Seq[(LfContractId, ReassignmentCounter)],
      targetStatus: Map[LfContractId, ContractState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Seq[(LfContractId, ReassignmentCounter)]] = {
    val filteredE =
      sourceContracts
        .traverse { case (cid, reassignmentCounter) =>
          val targetStatusOfContract = targetStatus.get(cid).map(_.status)

          targetStatusOfContract match {

            // Contract is inactive on the target synchronizer
            case None | Some(_: ActiveContractStore.ReassignedAway) |
                Some(ActiveContractStore.Purged) =>
              Right((cid, reassignmentCounter))

            case Some(targetState) =>
              Left(
                s"Active contract $cid in source synchronizer exists in target synchronizer with status $targetState. Use 'repair.add' or 'repair.purge' instead."
              )
          }
        }

    for {
      filtered <- EitherT.fromEither[FutureUnlessShutdown](filteredE)
      filteredContractIds = filtered.map(_._1)
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
      repairTarget.unwrap.synchronizer.topologySnapshot
        .hostedOn(stakeholders, participantId)
        .map(_.keySet)
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
      contractIds: Seq[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, List[SerializableContract]] =
    contractStore
      .lookupManyExistingUncached(contractIds)
      .leftMap(contractId =>
        s"Failed to look up contract $contractId in synchronizer $sourceSynchronizerAlias"
      )

  private def readContracts(
      contractIds: Seq[(LfContractId, ReassignmentCounter)]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Changes] =
    readContractsInstances(contractIds.map(_._1))
      .flatMap {
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
          } yield (contractId, serializableContract, serializedTargetO.isEmpty)
        }
      }
      .flatMap { changed =>
        val contractsById = changed.view.map { case (cid, contract, _) => cid -> contract }.toMap
        val newContractIds = changed.view.collect { case (cid, contract, true) => cid }.toSet

        val contractCounters =
          contractIds.flatMap { case (cid, counter) =>
            contractsById.get(cid).map { case contract => (contract, counter) }
          }
        val batches = ContractsReassignmentBatch.partition(contractCounters)
        EitherT.rightT[FutureUnlessShutdown, String](Changes(batches, newContractIds))
      }

  /** Write contracts in [[ContractStore]]
    */
  private def persistContracts(changes: Changes)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.right {
      changes.batches.parTraverse_ { case batch =>
        contractStore.storeContracts(batch.contracts.collect {
          case c if changes.isNew(c.contract.contractId) => c.contract
        })
      }
    }

  private def persistAssignments(
      contracts: Iterable[(LfContractId, Target[TimeOfRepair], ReassignmentCounter)]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] =
    targetPersistentState.unwrap.activeContractStore
      .assignContracts(
        contracts.map { case (cid, tor, reassignmentCounter) =>
          (
            cid,
            sourceSynchronizerId,
            reassignmentCounter,
            tor.unwrap.toToc,
          )
        }.toSeq
      )
      .mapAbort(e => s"Failed to mark contracts as assigned: $e")

  private def persistUnassignAndAssign(
      changes: ChangeAssignation.Data[Changes]
  )(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, String, ActiveContractStore.AcsWarning, Unit] = {

    val unassignF = sourcePersistentState.unwrap.activeContractStore
      .unassignContracts(
        changes.payload.batches.flatMap { case batch =>
          batch.contractIdCounters.map { case (contractId, reassignmentCounter) =>
            (
              contractId,
              targetSynchronizerId,
              reassignmentCounter,
              changes.sourceTimeOfRepair.unwrap.toToc,
            )
          }
        }
      )
      .mapAbort(e => s"Failed to mark contracts as unassigned: $e")

    unassignF
      .flatMap(_ =>
        persistAssignments(
          changes.payload.batches.flatMap { case batch =>
            batch.contractIdCounters.map { case (contractId, reassignmentCounter) =>
              (contractId, changes.targetTimeOfRepair, reassignmentCounter)
            }
          }
        )
      )
  }

  private def publishAssignmentEvent(
      reassignmentId: ReassignmentId,
      changes: ChangeAssignation.Data[Changes],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updates = assignment(reassignmentId, changes)

    for {
      _ <- FutureUnlessShutdown.outcomeF(
        MonadUtil.sequentialTraverse(updates)(repairIndexer.offer(_))
      )
    } yield ()
  }

  private def publishReassignmentEvents(
      changes: ChangeAssignation.Data[Changes]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val reassignmentId = ReassignmentId(sourceSynchronizerId, repairSource.unwrap.timestamp)
    val updates = unassignment(reassignmentId, changes) ++ assignment(reassignmentId, changes)
    for {
      _ <- FutureUnlessShutdown.outcomeF(
        MonadUtil.sequentialTraverse(updates)(repairIndexer.offer(_))
      )
    } yield ()
  }

  private def unassignment(
      reassignmentId: ReassignmentId,
      changes: ChangeAssignation.Data[Changes],
  )(implicit
      traceContext: TraceContext
  ): Seq[RepairUpdate] =
    changes.payload.batches.map { case batch =>
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourceSynchronizerId,
          targetSynchronizer = targetSynchronizerId,
          submitter = None,
          unassignId = reassignmentId.unassignmentTs,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Batch(batch.contracts.zipWithIndex.map { case (reassign, idx) =>
          Reassignment.Unassign(
            contractId = reassign.contract.contractId,
            templateId = reassign.templateId,
            packageName = reassign.packageName,
            stakeholders = batch.stakeholders.all,
            assignmentExclusivity = None,
            reassignmentCounter = reassign.counter.v,
            nodeId = idx,
          )
        }),
        repairCounter = changes.sourceTimeOfRepair.unwrap.repairCounter,
        recordTime = changes.sourceTimeOfRepair.unwrap.timestamp,
        synchronizerId = sourceSynchronizerId.unwrap,
      )
    }

  private def assignment(reassignmentId: ReassignmentId, changes: ChangeAssignation.Data[Changes])(
      implicit traceContext: TraceContext
  ): Seq[RepairUpdate] =
    changes.payload.batches.map { case batch =>
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourceSynchronizerId,
          targetSynchronizer = targetSynchronizerId,
          submitter = None,
          unassignId = reassignmentId.unassignmentTs,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Batch(batch.contracts.zipWithIndex.map { case (reassign, idx) =>
          Reassignment.Assign(
            ledgerEffectiveTime = reassign.contract.ledgerCreateTime.toLf,
            createNode = reassign.contract.toLf,
            contractMetadata = Bytes.fromByteString(
              reassign.contract.metadata
                .toByteString(repairTarget.unwrap.synchronizer.parameters.protocolVersion)
            ),
            reassignmentCounter = reassign.counter.v,
            nodeId = idx,
          )
        }),
        repairCounter = changes.targetTimeOfRepair.unwrap.repairCounter,
        recordTime = changes.targetTimeOfRepair.unwrap.timestamp,
        synchronizerId = targetSynchronizerId.unwrap,
      )
    }
}

// TODO(i14540): this needs to be called by RepairService to commit the changes
private[repair] object ChangeAssignation {

  final case class Data[Payload](
      payload: Payload,
      sourceTimeOfRepair: Source[TimeOfRepair],
      targetTimeOfRepair: Target[TimeOfRepair],
  ) {
    def incrementRepairCounter: Either[String, Data[Payload]] =
      for {
        incrementedSourceToc <- sourceTimeOfRepair.unwrap.incrementRepairCounter
        incrementedTargetToc <- targetTimeOfRepair.unwrap.incrementRepairCounter
      } yield copy(
        sourceTimeOfRepair = Source(incrementedSourceToc),
        targetTimeOfRepair = Target(incrementedTargetToc),
      )

    def map[T](f: Payload => T): Data[T] = this.copy(payload = f(payload))
  }

  object Data {

    implicit val dataFunctorInstance: Functor[Data] = new Functor[Data] {
      // Define the map function for Data
      override def map[A, B](fa: Data[A])(f: A => B): Data[B] =
        Data(f(fa.payload), fa.sourceTimeOfRepair, fa.targetTimeOfRepair)
    }

    def from[Payload](payload: Payload, changeAssignation: ChangeAssignation): Data[Payload] =
      ChangeAssignation
        .Data(
          payload,
          changeAssignation.repairSource.map(_.firstTimeOfRepair),
          changeAssignation.repairTarget.map(_.firstTimeOfRepair),
        )
  }

  /** @param contract
    *   Contracts that are reassigned, in batches
    * @param isNew
    *   Contract ids of the contract that were not seen before.
    */
  final case class Changes(
      batches: Seq[ContractsReassignmentBatch],
      isNew: Set[LfContractId],
  )
}
