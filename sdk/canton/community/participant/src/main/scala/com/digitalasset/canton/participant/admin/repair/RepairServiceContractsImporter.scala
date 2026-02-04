// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.DedicatedRepresentativePackageId
import com.digitalasset.canton.ledger.participant.state.Update.{
  ContractInfo,
  RepairReassignmentAccepted,
}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  RepairUpdate,
  TransactionMeta,
  Update,
}
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
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.CreationTime
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source as PekkoSource}

import scala.concurrent.{ExecutionContext, Future}

/** Implements the ACS import repair comments
  */
final class RepairServiceContractsImporter(
    syncCrypto: SyncCryptoApiParticipantProvider,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    packageMetadataView: PackageMetadataView,
    contractStore: Eval[ContractStore],
    aliasManager: SynchronizerAliasManager,
    nodeParameters: ParticipantNodeParameters,
    helpers: RepairServiceHelpers,
    contractValidator: ContractValidator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private type MissingContract = ContractInstance
  private type MissingAssignment =
    (LfContractId, ReassignmentTag.Source[SynchronizerId], ReassignmentCounter, TimeOfChange)
  private type MissingAdd = (LfContractId, ReassignmentCounter, TimeOfChange)

  override protected def timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  /** Decide whether contract `repairContract` needs to be added to the stores.
    *
    * DB calls: None
    *
    * @param repairContract
    *   Imported contracts
    * @param acsState
    *   Current state of the contract
    * @param storedContract
    *   Current stored instance for the contract
    * @return
    *   Some if the contract needs to be added/imported, false otherwise
    */
  private def contractToAdd(
      repairContract: RepairContract,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[ContractInstance],
  )(implicit
      traceContext: TraceContext
  ): Either[String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId

    def addContract(
        reassigningFrom: Option[ReassignmentTag.Source[SynchronizerId]]
    ): Either[String, Option[ContractToAdd]] =
      ContractInstance
        .create(repairContract.contract)
        .map(contractInstance =>
          Option(
            ContractToAdd(
              contract = contractInstance,
              reassignmentCounter = repairContract.reassignmentCounter,
              reassigningFrom = reassigningFrom,
              representativePackageId = repairContract.representativePackageId,
            )
          )
        )

    acsState match {
      case None => addContract(reassigningFrom = None)

      case Some(ActiveContractStore.Active(_)) =>
        logger.debug(s"Skipping contract $contractId because it is already active")
        for {
          contractAlreadyThere <-
            storedContract.toRight {
              s"Contract ${repairContract.contract.contractId} is active but is not found in the stores"
            }
          _ <- EitherUtil.condUnit(
            contractAlreadyThere.inst == repairContract.contract,
            s"Contract $contractId exists in synchronizer, but does not match with contract being added. "
              + s"Existing contract is $contractAlreadyThere while contract supposed to be added ${repairContract.contract}",
          )
        } yield None
      case Some(ActiveContractStore.Archived) =>
        Left(
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
          Left(
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
    *
    * Note: Assigning the internal contract ids to the contracts requires that all the contracts are
    * already persisted in the contract store.
    */
  def addContracts(
      synchronizerAlias: SynchronizerAlias,
      contracts: Seq[RepairContract],
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String] = None,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Adding ${contracts.length} contracts to synchronizer $synchronizerAlias"
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
            filteredContracts <- EitherT.fromEither[FutureUnlessShutdown](
              contractsWithOverriddenRpId.zip(contractStates).traverseFilter {
                case (contract, acsState) =>
                  contractToAdd(
                    repairContract = contract,
                    acsState = acsState,
                    storedContract = storedContracts.get(contract.contract.contractId),
                  )
              }
            )

            _ <- EitherT.fromEither[FutureUnlessShutdown](addContractsCheck(filteredContracts))

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
                        contractStore.value.lookupBatchedInternalIdsNonReadThrough(
                          contractsWithTimeOfChange.map(_._1.contract.contractId)
                        ),
                        "Unable to lookup internal contract ids in contract store",
                      )
                    contractsToAddWithInternalIds = checked(
                      tryAddInternalContractIds(
                        contractsToAdd,
                        internalContractIdsForContractsAdded,
                      )
                    )

                    _ <-
                      if (nodeParameters.alphaMultiSynchronizerSupport) {
                        // Publish added contracts via the indexer to the ledger api.
                        publishAddEvents(
                          synchronizer.psid.logical,
                          recordTime = synchronizer.currentRecordTime,
                          contractsToAddWithInternalIds,
                          workflowIds,
                          repairIndexer,
                        )
                      } else {
                        // Commit and publish added contracts via the indexer to the ledger api.
                        EitherT.right[String](
                          writeContractsAddedEvents(
                            synchronizer.psid.logical,
                            recordTime = synchronizer.currentRecordTime,
                            contractsToAddWithInternalIds,
                            workflowIds,
                            repairIndexer,
                          )
                        )
                      }
                  } yield ()
                },
              )
          } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("addContracts"))
        },
      )
    }
  }

  // This function requires that all contracts in contractsToAdd are already present in the
  // contract store and therefore their internal contract ids can be looked up.
  private def tryAddInternalContractIds(
      contractsToAdd: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[ContractToAdd]))],
      internalContractIds: Map[LfContractId, Long],
  )(implicit
      traceContext: TraceContext
  ): Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))] =
    contractsToAdd.map { case (timeOfRepair, (createdAt, batch)) =>
      val batchWithInternalIds = batch.map { contractToAdd =>
        val internalContractId =
          internalContractIds.getOrElse(
            contractToAdd.cid,
            ErrorUtil
              .invalidState(
                s"Not found internal contract id for contract ${contractToAdd.cid}"
              ),
          )
        (contractToAdd, internalContractId)
      }
      (timeOfRepair, (createdAt, batchWithInternalIds))
    }

  def addContractsPekko(
      synchronizerId: SynchronizerId,
      contracts: PekkoSource[RepairContract, NotUsed],
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String] = None,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val parameters = Map(
      "workflowIdPrefix" -> workflowIdPrefix.toString
    )

    logger.info(s"Adding contracts to synchronizer $synchronizerId with parameters: $parameters")

    def toFuture[T](resET: EitherT[FutureUnlessShutdown, String, T]): Future[T] = resET.value
      .flatMap(
        _.fold(
          err => FutureUnlessShutdown.failed[T](ImportAcsError.Error(err).asGrpcError),
          res => FutureUnlessShutdown.pure(res),
        )
      )
      .failOnShutdownToAbortException("addContractsPekko")

    val workflowProvider =
      Iterator
        .from(1)
        .map(i => workflowIdPrefix.map(prefix => LfWorkflowId.assertFromString(s"$prefix-$i")))

    val selectRepresentativePackageIds = new SelectRepresentativePackageIds(
      representativePackageIdOverride = representativePackageIdOverride,
      knownPackages = packageMetadataSnapshot.packages.keySet,
      packageNameMap = packageMetadataSnapshot.packageNameMap,
      contractImportMode = contractImportMode,
      loggerFactory = loggerFactory,
    )

    val batchSize = nodeParameters.batchingConfig.maxAcsImportBatchSize.unwrap
    val parallelism = nodeParameters.batchingConfig.parallelism.unwrap

    helpers.withRepairIndexer { repairIndexer =>
      val indexedContractBatches: PekkoSource[(Seq[RepairContract], Long), NotUsed] =
        contracts.grouped(batchSize).zipWithIndex

      val doneF = toFuture(helpers.readSynchronizerData(synchronizerId)).flatMap { synchronizer =>
        indexedContractBatches
          .mapAsync(parallelism) { data =>
            toFuture(
              validateAndPersistContracts(
                synchronizer = synchronizer,
                contractImportMode = contractImportMode,
                selectRepresentativePackageIds = selectRepresentativePackageIds,
                batchSize = batchSize,
              )(data)
            )
          }
          // Publish events to the indexer
          .mapAsync(1) { contractsToAddWithInternalContractIds =>
            if (nodeParameters.alphaMultiSynchronizerSupport) {
              toFuture(
                publishAddEvents(
                  synchronizerId,
                  synchronizer.currentRecordTime,
                  contractsToAddWithInternalContractIds,
                  workflowProvider,
                  repairIndexer,
                )
              )
            } else {
              writeContractsAddedEvents(
                synchronizerId,
                recordTime = synchronizer.currentRecordTime,
                contractsToAddWithInternalContractIds,
                workflowProvider,
                repairIndexer,
              ).failOnShutdownToAbortException("addContracts")
            }
          }
          .toMat(Sink.ignore)(Keep.right)
          .run()
      }

      EitherT.liftF(doneF.map(_ => ()))
    }
  }

  /** Validate the contracts in the batch and insert data in Canton stores.
    * @param batchSize
    *   Should correspond to the batch size used in the source
    * @return
    */
  private def validateAndPersistContracts(
      synchronizer: RepairRequest.SynchronizerData,
      contractImportMode: ContractImportMode,
      selectRepresentativePackageIds: SelectRepresentativePackageIds,
      batchSize: Int,
  )(
      data: (Seq[RepairContract], Long)
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
  ] = {
    val (contractsUnchecked, idx) = data

    for {
      contracts <- selectRepresentativePackageIds(contractsUnchecked)
        .toEitherT[FutureUnlessShutdown]

      contractsWithUnexpectedReassignmentCounter =
        if (nodeParameters.alphaMultiSynchronizerSupport) {
          Nil
        } else {
          contracts.filter(_.reassignmentCounter != ReassignmentCounter.Genesis)
        }

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        contractsWithUnexpectedReassignmentCounter.isEmpty,
        s"Contracts with a non-zero reassignment counter found with disabled multi-synchronizer support: ${contractsWithUnexpectedReassignmentCounter
            .map(c => (c.contractId, c.reassignmentCounter))}",
      )

      contractsWithWrongSynchronizer = contracts.filter(
        _.synchronizerId != synchronizer.psid.logical
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        contractsWithWrongSynchronizer.isEmpty,
        s"Contracts with wrong synchronizer: expected=${synchronizer.psid.logical}, actual=${contractsWithWrongSynchronizer
            .map(c => (c.contractId, c.synchronizerId))}",
      )

      _ <- ContractAuthenticationImportProcessor.validate(
        loggerFactory,
        syncPersistentStateLookup,
        contractValidator,
        contractImportMode,
      )(contracts)

      contractStates <- EitherT.right[String](
        helpers.readContractAcsStates(
          synchronizer.persistentState,
          contracts.map(_.contract.contractId),
        )
      )

      contractInstances <-
        helpers
          .logOnFailureWithInfoLevel(
            contractStore.value.lookupManyUncached(contracts.map(_.contract.contractId)),
            "Unable to lookup contracts in contract store",
          )
          .map(_.flatten)

      storedContracts = contractInstances.map(c => c.contractId -> c).toMap
      filteredContracts <- EitherT.fromEither[FutureUnlessShutdown](
        contracts.zip(contractStates).traverseFilter { case (contract, acsState) =>
          contractToAdd(
            repairContract = contract,
            acsState = acsState,
            storedContract = storedContracts.get(contract.contract.contractId),
          )
        }
      )

      _ <- EitherT.fromEither[FutureUnlessShutdown](addContractsCheck(filteredContracts))

      contractsByCreationTime = filteredContracts
        .groupBy(_.contract.inst.createdAt)
        .toList
        .sortBy { case (ledgerCreateTime, _) => ledgerCreateTime.time }

      /*
      Repair counters need to be strictly increasing but gaps are allowed.
      - monotonicity is guaranteed by pekko ordering guarantee
      - gaps can happen because of the grouping per create time
       */
      timesOfRepair = Iterator
        .from(idx.toInt * batchSize, 1)
        .map(i => TimeOfRepair(synchronizer.currentRecordTime, synchronizer.nextRepairCounter + i))

      contractsToAdd = timesOfRepair.zip(contractsByCreationTime).toSeq

      _ = logger.debug(s"Persisting ${filteredContracts.size} added contracts")

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
          contractStore.value.lookupBatchedInternalIdsNonReadThrough(
            contractsWithTimeOfChange.map(_._1.contract.contractId)
          ),
          "Unable to lookup internal contract ids in contract store",
        )

    } yield checked(tryAddInternalContractIds(contractsToAdd, internalContractIdsForContractsAdded))
  }

  /** Checks that the contracts can be added (packages known, contract keys have maintainers)
    */
  private def addContractsCheck(
      contracts: Seq[ContractToAdd]
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    for {
      // Check that the representative package-id is known
      _ <- contracts.map(_.representativePackageId).distinct.traverse(packageKnown)

      _ <- contracts.traverse { contractToAdd =>
        val contract = contractToAdd.contract
        val contractId = contractToAdd.cid
        EitherUtil.condUnit(
          !contract.contractKeyWithMaintainers.exists(_.maintainers.isEmpty),
          s"Contract $contractId has key without maintainers.",
        )
      }
    } yield ()

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
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // We compute first which changes we need to persist
    val missingContractsE: Either[String, Seq[MissingContract]] =
      contractsToAdd.traverseFilter[Either[String, *], MissingContract] { case (contractToAdd, _) =>
        storedContracts.get(contractToAdd.cid) match {
          case None => Right(Some(contractToAdd.contract))
          case Some(storedContract) =>
            Either.cond(
              storedContract == contractToAdd.contract,
              Option.empty[MissingContract],
              s"Contract ${contractToAdd.cid} already exists in the contract store, but differs from contract to be created. Contract to be created $contractToAdd versus existing contract $storedContract.",
            )
        }
      }

    for {
      // We compute first which changes we need to persist
      missingContracts <- EitherT.fromEither[FutureUnlessShutdown](missingContractsE)

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
  }

  private def prepareAddedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      ledgerCreateTime: CreationTime.CreatedAt,
      contractsAdded: Seq[(ContractToAdd, Long)],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): RepairUpdate = {
    val contractInfos = contractsAdded.view.map { case (c, internalContractId) =>
      val cid = c.contract.contractId
      cid -> ContractInfo(
        internalContractId = internalContractId,
        contractAuthenticationData = c.authenticationData,
        representativePackageId = DedicatedRepresentativePackageId(c.representativePackageId),
      )
    }.toMap
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contractsAdded.map(_._1.contract.toLf)).toMap
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
      synchronizerId = synchronizerId,
      repairCounter = repairCounter,
      recordTime = recordTime,
      contractInfos = contractInfos,
    )
  }

  private def writeContractsAddedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      contractsAdded: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
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
              workflowIdProvider = () => workflowIds.next(),
            )
          )
          .map(_ => ())
    })

  /** Build assignment events to be offered to the indexer to signal imported contracts. Assigned
    * event allows to preserve reassignment counters and shall be used until we have a dedicated
    * `add` event in the indexer.
    * @return
    *   - None if `contractsAdded` is empty
    *   - Some(Left) if contract authentication data cannot be computed
    *   - Some(Right) otherwise
    */
  private def prepareAssignedEvent(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      ledgerCreateTime: CreationTime.CreatedAt,
      contractsAdded: Seq[(ContractToAdd, Long)],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): Option[Either[String, RepairReassignmentAccepted]] = {

    // Assignments set the same source and target synchronizerIds since they are artificial
    // assigns without an actual target synchronizer (this is used for adding a contract)
    val reassignmentId = ReassignmentId(
      Source(synchronizerId),
      Target(synchronizerId),
      unassignmentTs = recordTime,
      contractIdCounters = contractsAdded.view.map { case (c, _internalContractId) =>
        (c.cid, c.reassignmentCounter)
      },
    )

    val assigns = contractsAdded.zipWithIndex
      .traverse { case ((c, internalContractId), nodeId) =>
        c.contract.contractAuthenticationData.map { contractAuthenticationData =>
          Reassignment.Assign(
            ledgerEffectiveTime = ledgerCreateTime.time,
            createNode = c.contract.toLf,
            contractAuthenticationData = contractAuthenticationData.toLfBytes,
            reassignmentCounter = c.reassignmentCounter.unwrap,
            nodeId = nodeId,
            internalContractId = internalContractId,
          )
        }
      }
      .map(NonEmpty.from)

    assigns.traverse(_.map { assignsNE =>
      RepairReassignmentAccepted(
        workflowId = workflowIdProvider(),
        updateId = randomUpdateId(syncCrypto),
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizerId),
          targetSynchronizer = Target(synchronizerId),
          submitter = None,
          reassignmentId = reassignmentId,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Batch(assignsNE),
        repairCounter = repairCounter,
        recordTime = recordTime,
        synchronizerId = synchronizerId,
      )
    })
  }

  private def publishAddEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      contractsAdded: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    MonadUtil
      .sequentialTraverse_(contractsAdded) { case (timeOfChange, (timestamp, contractsToAdd)) =>
        prepareAssignedEvent(
          synchronizerId,
          recordTime,
          timeOfChange.repairCounter,
          timestamp,
          contractsToAdd,
          () => workflowIds.next(),
        ) match {
          case Some(value) =>
            EitherT(value.traverse(repairIndexer.offer)).map(_ => ())

          case None => EitherTUtil.unit[String]
        }
      }
      .mapK(FutureUnlessShutdown.outcomeK)

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
