// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageIds
import com.digitalasset.canton.ledger.participant.state.{RepairUpdate, TransactionMeta, Update}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceContractsImporter.{
  ContractToAdd,
  workflowIdsFromPrefix,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.CreationTime

import scala.concurrent.ExecutionContext

/** Implements the ACS import repair comments
  */
final class RepairServiceContractsImporter(
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiParticipantProvider,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    packageMetadataView: PackageMetadataView,
    contractStore: Eval[ContractStore],
    aliasManager: SynchronizerAliasManager,
    parameters: ParticipantNodeParameters,
    helpers: RepairServiceHelpers,
    contractValidator: ContractValidator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private type MissingContract = ContractInstance
  private type MissingAssignment =
    (LfContractId, ReassignmentTag.Source[SynchronizerId], ReassignmentCounter, TimeOfChange)
  private type MissingAdd = (LfContractId, ReassignmentCounter, TimeOfChange)

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** Decide whether contract `repairContract` needs to be added to the stores.
    *
    * DB calls: None
    *
    * @param repairContract
    *   Imported contracts
    * @param ignoreAlreadyAdded
    *   If false, fails if the contract already exists in the store.
    * @param acsState
    *   Current state of the contract
    * @param storedContract
    *   Current stored instance for the contract
    * @return
    *   Some if the contract needs to be added/imported, false otherwise
    */
  private def contractToAdd(
      repairContract: RepairContract,
      ignoreAlreadyAdded: Boolean,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[ContractInstance],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId

    def addContract(
        reassigningFrom: Option[ReassignmentTag.Source[SynchronizerId]]
    ): EitherT[FutureUnlessShutdown, String, Option[ContractToAdd]] =
      for {
        contractInstance <- EitherT.fromEither[FutureUnlessShutdown](
          ContractInstance.create(repairContract.contract)
        )
      } yield Option(
        ContractToAdd(
          contract = contractInstance,
          reassignmentCounter = repairContract.reassignmentCounter,
          reassigningFrom = reassigningFrom,
          representativePackageId = repairContract.representativePackageId,
        )
      )

    acsState match {
      case None => addContract(reassigningFrom = None)

      case Some(ActiveContractStore.Active(_)) =>
        if (ignoreAlreadyAdded) {
          logger.debug(s"Skipping contract $contractId because it is already active")
          for {
            contractAlreadyThere <- EitherT.fromEither[FutureUnlessShutdown](
              storedContract.toRight {
                s"Contract ${repairContract.contract.contractId} is active but is not found in the stores"
              }
            )
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              contractAlreadyThere.inst == repairContract.contract,
              s"Contract $contractId exists in synchronizer, but does not match with contract being added. "
                + s"Existing contract is $contractAlreadyThere while contract supposed to be added ${repairContract.contract}",
            )
          } yield None
        } else {
          EitherT.leftT(
            s"A contract with $contractId is already active. Set ignoreAlreadyAdded = true to skip active contracts."
          )
        }
      case Some(ActiveContractStore.Archived) =>
        EitherT.leftT(
          s"Cannot add previously archived contract ${repairContract.contract.contractId} as archived contracts cannot become active."
        )
      case Some(ActiveContractStore.Purged) => addContract(reassigningFrom = None)
      case Some(ActiveContractStore.ReassignedAway(targetSynchronizer, reassignmentCounter)) =>
        logger.info(
          s"Marking contract ${repairContract.contract.contractId} previously unassigned targeting $targetSynchronizer as " +
            s"assigned from $targetSynchronizer (even though contract may have been reassigned to yet another synchronizer since)."
        )

        val isReassignmentCounterIncreasing =
          repairContract.reassignmentCounter > reassignmentCounter

        if (isReassignmentCounterIncreasing) {
          addContract(reassigningFrom = Option(ReassignmentTag.Source(targetSynchronizer.unwrap)))
        } else {
          EitherT.leftT(
            s"The reassignment counter ${repairContract.reassignmentCounter} of the contract " +
              s"${repairContract.contract.contractId} needs to be strictly larger than the reassignment counter " +
              s"$reassignmentCounter at the time of the unassignment."
          )
        }
    }
  }

  /** Participant repair utility for manually adding contracts to a synchronizer in an offline
    * fashion.
    *
    * @param synchronizerAlias
    *   alias of synchronizer to add contracts to. The synchronizer needs to be configured, but
    *   disconnected to prevent race conditions.
    * @param contracts
    *   contracts to add. Relevant pieces of each contract: create-arguments (LfThinContractInst),
    *   template-id (LfThinContractInst), contractId, ledgerCreateTime, salt (to be added to
    *   SerializableContract), and witnesses, SerializableContract.metadata is only validated, but
    *   otherwise ignored as stakeholder and signatories can be recomputed from contracts.
    * @param ignoreAlreadyAdded
    *   whether to ignore and skip over contracts already added/present in the synchronizer. Setting
    *   this to true (at least on retries) enables writing idempotent repair scripts.
    * @param ignoreStakeholderCheck
    *   do not check for stakeholder presence for the given parties
    * @param contractImportMode
    *   Whether contract ids should be validated
    * @param packageMetadataSnapshot
    *   Snapshot of the packages metadata
    * @param representativePackageIdOverride
    *   Description for the override of the representative package ids
    * @param workflowIdPrefix
    *   If present, each transaction generated for added contracts will have a workflow ID whose
    *   prefix is the one set and the suffix is a sequential number and the number of transactions
    *   generated as part of the addition (e.g. `import-foo-1-2`, `import-foo-2-2`)
    */
  def addContracts(
      synchronizerAlias: SynchronizerAlias,
      contracts: Seq[RepairContract],
      ignoreAlreadyAdded: Boolean,
      ignoreStakeholderCheck: Boolean,
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String] = None,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Adding ${contracts.length} contracts to synchronizer $synchronizerAlias with ignoreAlreadyAdded=$ignoreAlreadyAdded and ignoreStakeholderCheck=$ignoreStakeholderCheck"
    )
    if (contracts.isEmpty) {
      Either.right(logger.info("No contracts to add specified"))
    } else {
      helpers.runConsecutiveAndAwaitUS(
        "repair.add",
        helpers.withRepairIndexer { repairIndexer =>
          val selectRepresentativePackageIds = new SelectRepresentativePackageIds(
            representativePackageIdOverride = representativePackageIdOverride,
            knownPackages = packageMetadataSnapshot.packages.keySet,
            packageNameMap = packageMetadataSnapshot.packageNameMap,
            contractImportMode = contractImportMode,
            loggerFactory = loggerFactory,
          )

          (for {
            synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
              aliasManager
                .synchronizerIdForAlias(synchronizerAlias)
                .toRight(s"Could not find $synchronizerAlias")
            )

            synchronizer <- helpers.readSynchronizerData(synchronizerId)

            contractsWithOverriddenRpId <- selectRepresentativePackageIds(contracts)
              .toEitherT[FutureUnlessShutdown]

            _ <- ContractAuthenticationImportProcessor.validate(
              loggerFactory,
              syncPersistentStateLookup,
              contractValidator,
              contractImportMode,
            )(contractsWithOverriddenRpId)

            contractStates <- EitherT.right[String](
              helpers.readContractAcsStates(
                synchronizer.persistentState,
                contractsWithOverriddenRpId.map(_.contract.contractId),
              )
            )

            contractInstances <-
              helpers
                .logOnFailureWithInfoLevel(
                  contractStore.value
                    .lookupManyUncached(contractsWithOverriddenRpId.map(_.contract.contractId)),
                  "Unable to lookup contracts in contract store",
                )
                .map(_.flatten)

            storedContracts = contractInstances.map(c => c.contractId -> c).toMap
            filteredContracts <- contractsWithOverriddenRpId.zip(contractStates).parTraverseFilter {
              case (contract, acsState) =>
                contractToAdd(
                  repairContract = contract,
                  ignoreAlreadyAdded = ignoreAlreadyAdded,
                  acsState = acsState,
                  storedContract = storedContracts.get(contract.contract.contractId),
                )
            }

            _ <- addContractsCheck(
              synchronizer,
              ignoreStakeholderCheck = ignoreStakeholderCheck,
              filteredContracts,
            )

            contractsByCreation = filteredContracts
              .groupBy(_.contract.inst.createdAt)
              .toList
              .sortBy { case (ledgerCreateTime, _) => ledgerCreateTime.time }

            _ <- PositiveInt
              .create(contractsByCreation.size)
              .fold(
                _ =>
                  EitherT.rightT[FutureUnlessShutdown, String](
                    logger.info("No contract needs to be added")
                  ),
                groupCount => {
                  val workflowIds = workflowIdsFromPrefix(workflowIdPrefix, groupCount)
                  for {
                    repair <- helpers.initRepairRequestAndVerifyPreconditions(
                      synchronizer = synchronizer,
                      repairCountersToAllocate = groupCount,
                    )

                    contractsToAdd = repair.timesOfRepair.zip(contractsByCreation)

                    _ = logger.debug(s"Publishing ${filteredContracts.size} added contracts")

                    contractsWithTimeOfChange = contractsToAdd.flatMap { case (tor, (_, cs)) =>
                      cs.map(_ -> tor.toToc)
                    }

                    _ <- persistAddContracts(
                      synchronizer,
                      contractsToAdd = contractsWithTimeOfChange,
                      storedContracts = storedContracts,
                    )

                    internalContractIdsForContractsAdded <-
                      helpers.logOnFailureWithInfoLevel(
                        contractStore.value.lookupBatchedNonCachedInternalIds(
                          contractsWithTimeOfChange.map(_._1.contract.contractId)
                        ),
                        "Unable to lookup internal contract ids in contract store",
                      )

                    // Commit and publish added contracts via the indexer to the ledger api.
                    _ <- EitherT.right[String](
                      writeContractsAddedEvents(
                        synchronizer.psid.logical,
                        recordTime = synchronizer.currentRecordTime,
                        contractsToAdd,
                        internalContractIdsForContractsAdded,
                        workflowIds,
                        repairIndexer,
                      )
                    )
                  } yield ()
                },
              )
          } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("addContracts"))
        },
      )
    }
  }

  /** Checks that the contracts can be added (packages known, stakeholders hosted, ...)
    */
  private def addContractsCheck(
      synchronizer: RepairRequest.SynchronizerData,
      ignoreStakeholderCheck: Boolean,
      contracts: Seq[ContractToAdd],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val allStakeholders = contracts
      .flatMap(_.contract.stakeholders)
      .toSet

    for {
      allLocallyHostedStakeholders <- EitherT.right(
        synchronizer.topologySnapshot
          .hostedOn(allStakeholders, participantId)
          .map(_.keySet)
      )

      allHostedStakeholders <- EitherT.liftF(
        synchronizer.topologySnapshot
          .allHaveActiveParticipants(allStakeholders)
          .fold(
            missingParties => allStakeholders.diff(missingParties),
            _ => allStakeholders,
          )
      )

      // Check that the representative package-id is known
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        contracts.map(_.representativePackageId).distinct.traverse(packageKnown)
      )

      _ <- MonadUtil.parTraverseWithLimit_(parameters.batchingConfig.parallelism)(contracts)(
        addContractChecks(
          synchronizer,
          allLocallyHostedStakeholders = allLocallyHostedStakeholders,
          allHostedStakeholders = allHostedStakeholders,
          ignoreStakeholderCheck = ignoreStakeholderCheck,
        )
      )
    } yield ()
  }

  /** Checks that one contract can be added (stakeholders hosted, ...)
    *
    * DB calls: None
    *
    * @param allLocallyHostedStakeholders
    *   All parties that are a stakeholder of one of the contracts and are hosted locally
    * @param allHostedStakeholders
    *   All parties that are a stakeholder of one of the contracts and are hosted on some
    *   participant
    */
  private def addContractChecks(
      synchronizer: RepairRequest.SynchronizerData,
      allLocallyHostedStakeholders: Set[LfPartyId],
      allHostedStakeholders: Set[LfPartyId],
      ignoreStakeholderCheck: Boolean,
  )(
      contractToAdd: ContractToAdd
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val contract = contractToAdd.contract
    val contractId = contractToAdd.cid
    for {
      _warnOnEmptyMaintainers <- EitherT.cond[FutureUnlessShutdown](
        !contract.contractKeyWithMaintainers.exists(_.maintainers.isEmpty),
        (),
        s"Contract $contractId has key without maintainers.",
      )

      _ <-
        if (ignoreStakeholderCheck) EitherT.rightT[FutureUnlessShutdown, String](())
        else {
          val localStakeholders =
            contractToAdd.contract.stakeholders.intersect(allLocallyHostedStakeholders)

          val missingHostedStakeholders =
            contractToAdd.contract.stakeholders.diff(allHostedStakeholders)

          for {
            // At least one stakeholder is hosted locally
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              localStakeholders.nonEmpty,
              s"Contract ${contract.contractId} has stakeholders ${contract.stakeholders} but none of them are hosted locally",
            )

            // All stakeholders exist on the synchronizer
            _ <- EitherT.cond[FutureUnlessShutdown](
              missingHostedStakeholders.isEmpty,
              (),
              s"Synchronizer ${synchronizer.psid} missing stakeholders $missingHostedStakeholders of contract ${contract.contractId}",
            )
          } yield ()
        }
    } yield ()
  }

  /** Actual persistence work
    * @param synchronizer
    *   Synchronizer data
    * @param contractsToAdd
    *   Contracts to be added
    * @param storedContracts
    *   Contracts that already exists in the store
    */
  private def persistAddContracts(
      synchronizer: RepairRequest.SynchronizerData,
      contractsToAdd: Seq[(ContractToAdd, TimeOfChange)],
      storedContracts: Map[LfContractId, ContractInstance],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      // We compute first which changes we need to persist
      missingContracts <- contractsToAdd
        .parTraverseFilter[EitherT[FutureUnlessShutdown, String, *], MissingContract] {
          case (contractToAdd, _) =>
            storedContracts.get(contractToAdd.cid) match {
              case None =>
                EitherT.rightT[FutureUnlessShutdown, String](Some(contractToAdd.contract))
              case Some(storedContract) =>
                EitherT.cond[FutureUnlessShutdown](
                  storedContract == contractToAdd.contract,
                  Option.empty[MissingContract],
                  s"Contract ${contractToAdd.cid} already exists in the contract store, but differs from contract to be created. Contract to be created $contractToAdd versus existing contract $storedContract.",
                )
            }
        }

      (missingAssignments, missingAdds) = contractsToAdd.foldLeft(
        (Seq.empty[MissingAssignment], Seq.empty[MissingAdd])
      ) { case ((missingAssignments, missingAdds), (contract, toc)) =>
        contract.reassigningFrom match {
          case Some(sourceSynchronizerId) =>
            val newAssignment =
              (contract.cid, sourceSynchronizerId, contract.reassignmentCounter, toc)
            (newAssignment +: missingAssignments, missingAdds)

          case None =>
            val newAdd = (contract.cid, contract.reassignmentCounter, toc)
            (missingAssignments, newAdd +: missingAdds)
        }
      }

      // Now, we update the stores
      _ <- helpers.logOnFailureWithInfoLevel(
        contractStore.value.storeContracts(missingContracts),
        "Unable to store missing contracts",
      )

      _ <- synchronizer.persistentState.activeContractStore
        .markContractsAdded(missingAdds)
        .toEitherTWithNonaborts
        .leftMap(e =>
          s"Failed to add contracts ${missingAdds.map { case (cid, _, _) => cid }} in ActiveContractStore: $e"
        )

      _ <- synchronizer.persistentState.activeContractStore
        .assignContracts(missingAssignments)
        .toEitherTWithNonaborts
        .leftMap(e =>
          s"Failed to assign ${missingAssignments.map { case (cid, _, _, _) => cid }} in ActiveContractStore: $e"
        )
    } yield ()

  private def prepareAddedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      ledgerCreateTime: CreationTime.CreatedAt,
      contractsAdded: Seq[ContractToAdd],
      internalContractIdsForContractsAdded: Map[LfContractId, Long],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): RepairUpdate = {
    val contractAuthenticationData = contractsAdded.view.map { c =>
      c.contract.contractId -> c.authenticationData
    }.toMap
    val representativePackageIds = contractsAdded.view
      .map(c => c.contract.contractId -> c.representativePackageId)
      .toMap
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contractsAdded.map(_.contract.toLf)).toMap
    Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = ledgerCreateTime.time,
        workflowId = workflowIdProvider(),
        preparationTime = recordTime.toLf,
        submissionSeed = Update.noOpSeed,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = LfCommittedTransaction(
        CantonOnly.lfVersionedTransaction(
          nodes = txNodes,
          roots = ImmArray.from(nodeIds.take(txNodes.size)),
        )
      ),
      updateId = randomUpdateId(syncCrypto),
      contractAuthenticationData = contractAuthenticationData,
      representativePackageIds = RepresentativePackageIds.from(representativePackageIds),
      synchronizerId = synchronizerId,
      repairCounter = repairCounter,
      recordTime = recordTime,
      internalContractIds = internalContractIdsForContractsAdded,
    )
  }

  private def writeContractsAddedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      contractsAdded: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[ContractToAdd]))],
      internalContractIdsForContractsAdded: Map[LfContractId, Long],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(MonadUtil.sequentialTraverse_(contractsAdded) {
      case (timeOfChange, (timestamp, contractsToAdd)) =>
        // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
        repairIndexer
          .offer(
            prepareAddedEvents(
              synchronizerId = synchronizerId,
              recordTime = recordTime,
              repairCounter = timeOfChange.repairCounter,
              ledgerCreateTime = timestamp,
              contractsAdded = contractsToAdd,
              internalContractIdsForContractsAdded = internalContractIdsForContractsAdded,
              workflowIdProvider = () => workflowIds.next(),
            )
          )
          .map(_ => ())
    })

  private def packageKnown(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    Either.cond(
      packageMetadataView.getSnapshot.packages.contains(lfPackageId),
      (),
      s"Failed to locate package $lfPackageId",
    )
}

object RepairServiceContractsImporter {

  private final case class ContractToAdd(
      contract: ContractInstance,
      reassignmentCounter: ReassignmentCounter,
      reassigningFrom: Option[ReassignmentTag.Source[SynchronizerId]],
      representativePackageId: LfPackageId,
  ) {
    def cid: LfContractId = contract.contractId

    def authenticationData: Bytes =
      contract.inst.authenticationData
  }

  /** Generate workflow IDs from a given prefix. Allow to correlate updates with the repair
    * operation
    */
  private def workflowIdsFromPrefix(
      prefix: Option[String],
      n: PositiveInt,
  ): Iterator[Option[LfWorkflowId]] =
    prefix.fold(
      Iterator.continually(Option.empty[LfWorkflowId])
    )(prefix =>
      1.to(n.value)
        .map(i => Some(LfWorkflowId.assertFromString(s"$prefix-$i-${n.value}")))
        .iterator
    )

}
