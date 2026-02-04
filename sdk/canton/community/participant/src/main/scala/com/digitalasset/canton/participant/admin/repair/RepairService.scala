// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.ledger.participant.state.Update.RepairReassignmentAccepted
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
import com.digitalasset.canton.participant.admin.repair.RepairService.PurgeOperations
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncEphemeralStateFactory,
  SyncPersistentStateLookup,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfChoiceName, *}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.ImmArray
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

/** Implements the repair commands. Repair commands work only if the participant has disconnected
  * from the affected synchronizers. Additionally for repair commands, which change the Ledger API
  * events: all synchronizers needs to be disconnected, and the indexer is switched to repair mode.
  * Every individual repair command is executed transactionally, i.e., either all its effects are
  * applied or none. This is achieved by the repair-indexer only changing the Synchronizer Indexes
  * for the affected synchronizers after all previous operations were successful, and the emitted
  * Update events are all persisted. In case of an error during repair, or crash during repair: on
  * node and synchronizer recovery all the changes will be purged. During a repair operation (as
  * synchronizers are disconnected) no new events are visible on the Ledger API, neither the ones
  * added by the ongoing repair. As the repair operation successfully finished new events (if any)
  * will become visible on the Ledger API - Ledger End and synchronizer indexes change, open tailing
  * streams start emitting the repair events if applicable.
  *
  * @param executionQueue
  *   Sequential execution queue on which repair actions must be run. This queue is shared with the
  *   CantonSyncService, which uses it for synchronizer connections. Sharing it ensures that we
  *   cannot connect to the synchronizer while a repair action is running and vice versa. It also
  *   ensure only one repair runs at a time. This ensures concurrent activity among repair
  *   operations does not corrupt state.
  */
final class RepairService(
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiParticipantProvider,
    packageMetadataView: PackageMetadataView,
    contractStore: Eval[ContractStore],
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    aliasManager: SynchronizerAliasManager,
    parameters: ParticipantNodeParameters,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    contractValidator: ContractValidator,
    @VisibleForTesting
    private[canton] val executionQueue: SimpleExecutionQueue,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val helpers = new RepairServiceHelpers(
    participantId,
    packageMetadataView,
    ledgerApiIndexer,
    parameters,
    syncPersistentStateLookup,
    connectedSynchronizersLookup,
    executionQueue,
    loggerFactory,
  )

  private val contractsImporter = new RepairServiceContractsImporter(
    syncCrypto,
    syncPersistentStateLookup,
    packageMetadataView,
    contractStore,
    aliasManager,
    parameters,
    helpers,
    contractValidator,
    loggerFactory,
  )

  private def synchronizerNotConnected(
      psid: PhysicalSynchronizerId
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.cond(
      !connectedSynchronizersLookup.isConnected(psid),
      (),
      s"Participant is still connected to synchronizer $psid",
    )

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
    */
  def addContracts(
      synchronizerAlias: SynchronizerAlias,
      contracts: Seq[RepairContract],
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String],
  )(implicit traceContext: TraceContext): Either[String, Unit] = contractsImporter.addContracts(
    synchronizerAlias = synchronizerAlias,
    contracts = contracts,
    contractImportMode = contractImportMode,
    packageMetadataSnapshot = packageMetadataSnapshot,
    representativePackageIdOverride = representativePackageIdOverride,
    workflowIdPrefix = workflowIdPrefix,
  )

  // TODO(#30342) - Consolidate with addContracts, or separate it clearly
  def addContractsPekko(
      synchronizerId: SynchronizerId,
      contracts: Source[RepairContract, NotUsed],
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    contractsImporter.addContractsPekko(
      synchronizerId = synchronizerId,
      contracts = contracts,
      contractImportMode = contractImportMode,
      packageMetadataSnapshot = packageMetadataSnapshot,
      representativePackageIdOverride = representativePackageIdOverride,
      workflowIdPrefix = workflowIdPrefix,
    )

  /** Participant repair utility for manually purging (archiving) contracts in an offline fashion.
    *
    * @param synchronizerAlias
    *   alias of synchronizer to purge contracts from. The synchronizer needs to be configured, but
    *   disconnected to prevent race conditions.
    * @param contractIds
    *   lf contract ids of contracts to purge
    * @param ignoreAlreadyPurged
    *   whether to ignore already purged contracts.
    */
  def purgeContracts(
      synchronizerAlias: SynchronizerAlias,
      contractIds: NonEmpty[Seq[LfContractId]],
      ignoreAlreadyPurged: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Purging ${contractIds.length} contracts from $synchronizerAlias with ignoreAlreadyPurged=$ignoreAlreadyPurged. " +
        s"Mode: ${if (parameters.alphaMultiSynchronizerSupport) "Alpha Multi Synchronizer (Unassignment)"
          else "Standard (Archive Transaction)"}"
    )

    helpers.runConsecutiveAndAwaitUS(
      "repair.purge",
      helpers.withRepairIndexer { repairIndexer =>
        (for {
          synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
            aliasManager
              .synchronizerIdForAlias(synchronizerAlias)
              .toRight(s"Could not find $synchronizerAlias")
          )

          repair <- helpers.initRepairRequestAndVerifyPreconditions(synchronizerId)

          contractStates <- EitherT.right[String](
            helpers.readContractAcsStates(
              repair.synchronizer.persistentState,
              contractIds,
            )
          )

          contractInstances <-
            helpers
              .logOnFailureWithInfoLevel(
                contractStore.value.lookupManyUncached(contractIds),
                "Unable to lookup contracts in contract store",
              )
              .map(_.flatten)

          storedContracts <- EitherT.fromEither[FutureUnlessShutdown](
            contractInstances
              .traverse { contract =>
                SerializableContract
                  .fromLfFatContractInst(contract.inst)
                  .map(c => c.contractId -> c)
              }
              .map(_.toMap)
          )

          toc = repair.tryExactlyOneTimeOfRepair.toToc

          _ <-
            if (parameters.alphaMultiSynchronizerSupport) {
              for {
                operationsE <- EitherT.fromEither[FutureUnlessShutdown](
                  contractIds
                    .zip(contractStates)
                    .foldMapM { case (cid, acsStatus) =>
                      val storedContract = storedContracts.get(cid)
                      computePurgeOperations(toc, ignoreAlreadyPurged)(
                        cid,
                        acsStatus,
                        storedContract,
                      )
                        .map { case PurgeOperations(missingPurge, missingAssignment, upstream) =>
                          (upstream.toList, missingPurge.toList, missingAssignment.toList)
                        }
                    }
                )

                (contractToDeactivateUpstream, missingPurges, missingAssignments) = operationsE

                // Update the stores
                _ <- repair.synchronizer.persistentState.activeContractStore
                  .purgeContracts(missingPurges)
                  .toEitherTWithNonaborts
                  .leftMap(e =>
                    s"Failed to purge contracts $missingAssignments in ActiveContractStore: $e"
                  )

                _ <- repair.synchronizer.persistentState.activeContractStore
                  .assignContracts(missingAssignments)
                  .toEitherTWithNonaborts
                  .leftMap(e =>
                    s"Failed to assign contracts $missingAssignments in ActiveContractStore: $e"
                  )

                // Publish purged contracts via the indexer to the ledger api.
                _ <- EitherTUtil.rightUS[String, Unit](
                  publishUnassignedEvent(contractToDeactivateUpstream, repair, repairIndexer)
                )
              } yield ()

            } else {
              for {
                updateId <- EitherT.rightT[FutureUnlessShutdown, String](randomUpdateId(syncCrypto))

                operationsE <- EitherT.fromEither[FutureUnlessShutdown](
                  contractIds
                    .zip(contractStates)
                    .foldMapM { case (cid, acsStatus) =>
                      val storedContract = storedContracts.get(cid)
                      computePurgeOperations(toc, ignoreAlreadyPurged)(
                        cid,
                        acsStatus,
                        storedContract,
                      )
                        .map { case PurgeOperations(missingPurge, missingAssignment, upstream) =>
                          // Extract only the SerializableContract from upstream, discard the reassignment counter
                          val contracts = upstream.map(_._1).toList
                          (contracts, missingPurge.toList, missingAssignment.toList)
                        }
                    }
                )

                (contractsToPublishUpstream, missingPurges, missingAssignments) = operationsE

                // Update the stores
                _ <- repair.synchronizer.persistentState.activeContractStore
                  .purgeContracts(missingPurges)
                  .toEitherTWithNonaborts
                  .leftMap(e =>
                    s"Failed to purge contracts $missingAssignments in ActiveContractStore: $e"
                  )

                _ <- repair.synchronizer.persistentState.activeContractStore
                  .assignContracts(missingAssignments)
                  .toEitherTWithNonaborts
                  .leftMap(e =>
                    s"Failed to assign contracts $missingAssignments in ActiveContractStore: $e"
                  )

                // Commit and publish purged contracts via the indexer to the ledger api.
                _ <- EitherTUtil.rightUS[String, Unit](
                  writeContractsPurgedEvent(
                    contractsToPublishUpstream,
                    updateId,
                    repair,
                    repairIndexer,
                  )
                )
              } yield ()
            }

        } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("purgeContracts"))
      },
    )
  }

  /** Change the assignation of a contract from one synchronizer to another
    *
    * This function here allows us to manually insert a unassignment/assignment into the respective
    * journals in order to change the assignation of a contract from one synchronizer to another.
    * The procedure will result in a consistent state if and only if all the counter parties run the
    * same command. Failure to do so, will results in participants reporting errors and possibly
    * break.
    *
    * @param contracts
    *   Contracts whose assignation should be changed. The reassignment counter is by default
    *   incremented by one. A non-empty reassignment counter allows to override the default behavior
    *   with the provided counter.
    * @param skipInactive
    *   If true, then the migration will skip contracts in the contractId list that are inactive
    */
  def changeAssignation(
      contracts: NonEmpty[Seq[(LfContractId, Option[ReassignmentCounter])]],
      sourceSynchronizer: ReassignmentTag.Source[SynchronizerId],
      targetSynchronizer: ReassignmentTag.Target[SynchronizerId],
      skipInactive: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val contractsCount = PositiveInt.tryCreate(contracts.size)
    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        sourceSynchronizer.unwrap != targetSynchronizer.unwrap,
        "Source must differ from target synchronizer!",
      )

      repairSource <- sourceSynchronizer.traverse(
        helpers.initRepairRequestAndVerifyPreconditions(
          _,
          contractsCount,
        )
      )

      repairTarget <- targetSynchronizer.traverse(
        helpers.initRepairRequestAndVerifyPreconditions(
          _,
          contractsCount,
        )
      )

      _ <- helpers.withRepairIndexer { repairIndexer =>
        val changeAssignation = new ChangeAssignation(
          repairSource,
          repairTarget,
          participantId,
          syncCrypto,
          repairIndexer,
          contractStore.value,
          loggerFactory,
        )
        (for {
          changeAssignationData <- EitherT.rightT[FutureUnlessShutdown, String](
            ChangeAssignation.Data.from(contracts.forgetNE, changeAssignation)
          )
          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          _ <- changeAssignation
            .changeAssignation(changeAssignationData, skipInactive)
            .map(_ => Seq[Unit]())

        } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("changeAssignation"))
      }
    } yield ()
  }

  def ignoreEvents(
      synchronizerId: PhysicalSynchronizerId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Ignoring sequenced events from $fromInclusive to $toInclusive (force = $force).")
    helpers.runConsecutive(
      "repair.skip_messages",
      for {
        _ <- synchronizerNotConnected(synchronizerId)
        _ <- performIfRangeSuitableForIgnoreOperations(synchronizerId, fromInclusive, force)(
          _.ignoreEvents(fromInclusive, toInclusive).leftMap(_.toString)
        )
      } yield (),
    )
  }

  /** Rollback the Unassignment. The contract is re-assigned to the source synchronizer. The
    * reassignment counter is increased by two. The contract is inserted into the contract store on
    * the target synchronizer if it is not already there. Additionally, we publish the reassignment
    * events.
    */
  def rollbackUnassignment(
      reassignmentId: ReassignmentId,
      source: ReassignmentTag.Source[SynchronizerId],
      target: ReassignmentTag.Target[SynchronizerId],
  )(implicit context: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    helpers.withRepairIndexer { repairIndexer =>
      (for {
        sourceRepairRequest <- source.traverse(helpers.initRepairRequestAndVerifyPreconditions(_))
        targetRepairRequest <- target.traverse(helpers.initRepairRequestAndVerifyPreconditions(_))
        reassignmentData <-
          targetRepairRequest.unwrap.synchronizer.persistentState.reassignmentStore
            .lookup(reassignmentId)
            .leftMap(_.message)

        changeAssignation = new ChangeAssignation(
          sourceRepairRequest,
          targetRepairRequest,
          participantId,
          syncCrypto,
          repairIndexer,
          contractStore.value,
          loggerFactory,
        )

        unassignmentData = ChangeAssignation.Data.from(reassignmentData, changeAssignation)
        _ <- changeAssignation.completeUnassigned(unassignmentData)

        changeAssignationBack = new ChangeAssignation(
          ReassignmentTag.Source(targetRepairRequest.unwrap),
          ReassignmentTag.Target(sourceRepairRequest.unwrap),
          participantId,
          syncCrypto,
          repairIndexer,
          contractStore.value,
          loggerFactory,
        )
        contractIdsData <- EitherT.fromEither[FutureUnlessShutdown](
          ChangeAssignation.Data
            .from[Seq[(LfContractId, Option[ReassignmentCounter])]](
              reassignmentData.contractsBatch.contractIds.map(_ -> None).toSeq,
              changeAssignationBack,
            )
            .incrementRepairCounter
        )
        _ <- changeAssignationBack.changeAssignation(
          contractIdsData,
          skipInactive = false,
        )
      } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("rollbackUnassignment"))
    }

  private def performIfRangeSuitableForIgnoreOperations[T](
      psid: PhysicalSynchronizerId,
      from: SequencerCounter,
      force: Boolean,
  )(
      action: SequencedEventStore => EitherT[FutureUnlessShutdown, String, T]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, T] =
    for {
      persistentState <- EitherT.fromEither[FutureUnlessShutdown](
        helpers.lookUpSynchronizerPersistence(psid)
      )
      _ <- EitherT.right(
        ledgerApiIndexer.value
          .ensureNoProcessingForSynchronizer(psid.logical)
      )
      synchronizerIndex <- EitherT.right(
        ledgerApiIndexer.value.ledgerApiStore.value.cleanSynchronizerIndex(psid.logical)
      )

      startingPoints <- EitherT.right(
        SyncEphemeralStateFactory.startingPoints(
          persistentState.requestJournalStore,
          persistentState.sequencedEventStore,
          synchronizerIndex,
        )
      )
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          force || startingPoints.processing.nextSequencerCounter <= from,
          show"Unable to modify events between $from (inclusive) and ${startingPoints.processing.nextSequencerCounter} (exclusive), " +
            """as they have already been processed. Enable "force" to modify them nevertheless.""",
        )
      res <- action(persistentState.sequencedEventStore)
    } yield res

  def unignoreEvents(
      synchronizerId: PhysicalSynchronizerId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(
      s"Unignoring sequenced events from $fromInclusive to $toInclusive (force = $force)."
    )
    helpers.runConsecutive(
      "repair.unskip_messages",
      for {
        _ <- synchronizerNotConnected(synchronizerId)
        _ <- performIfRangeSuitableForIgnoreOperations(synchronizerId, fromInclusive, force)(
          sequencedEventStore =>
            sequencedEventStore.unignoreEvents(fromInclusive, toInclusive).leftMap(_.toString)
        )
      } yield (),
    )
  }

  /** For the given contract, returns the operations (purge, assignment) to perform
    * @param acsStatus
    *   Status of the contract
    * @param storedContractO
    *   Instance of the contract
    */
  private def computePurgeOperations(toc: TimeOfChange, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId,
      acsStatus: Option[ActiveContractStore.Status],
      storedContractO: Option[SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): Either[String, PurgeOperations] = {
    def ignoreOrError(reason: String): Either[String, PurgeOperations] =
      Either.cond(
        ignoreAlreadyPurged,
        PurgeOperations.empty,
        s"Contract $cid cannot be purged: $reason. Set ignoreAlreadyPurged = true to skip non-existing contracts.",
      )

    // Not checking that the participant hosts a stakeholder as we might be cleaning up contracts
    // on behalf of stakeholders no longer around.
    acsStatus match {
      case None => ignoreOrError("unknown contract")
      case Some(ActiveContractStore.Active(reassignmentCounter)) =>
        for {
          _contract <- Either
            .fromOption(
              storedContractO,
              show"Active contract $cid not found in contract store",
            )
        } yield {
          PurgeOperations(
            purge = Option((cid, toc)),
            assign = None,
            upstream = storedContractO.map((_, reassignmentCounter)),
          )
        }
      case Some(ActiveContractStore.Archived) => ignoreOrError("archived contract")
      case Some(ActiveContractStore.Purged) => ignoreOrError("purged contract")
      case Some(ActiveContractStore.ReassignedAway(targetSynchronizer, reassignmentCounter)) =>
        logger.info(
          s"Purging contract $cid previously marked as reassigned to $targetSynchronizer. " +
            s"Marking contract as assigned from $targetSynchronizer (even though contract may have since been reassigned to yet another synchronizer) and subsequently as archived."
        )

        reassignmentCounter.increment.map { newReassignmentCounter =>
          PurgeOperations(
            purge = Option((cid, toc)),
            assign = Option(
              (
                cid,
                ReassignmentTag.Source(targetSynchronizer.unwrap),
                newReassignmentCounter,
                toc,
              )
            ),
            upstream = storedContractO.map((_, reassignmentCounter)),
          )
        }
    }
  }

  private def toArchive(c: SerializableContract): LfNodeExercises = LfNodeExercises(
    targetCoid = c.contractId,
    templateId = c.rawContractInstance.contractInstance.unversioned.template,
    packageName = c.rawContractInstance.contractInstance.unversioned.packageName,
    interfaceId = None,
    choiceId = LfChoiceName.assertFromString("Archive"),
    consuming = true,
    actingParties = c.metadata.signatories,
    chosenValue = c.rawContractInstance.contractInstance.unversioned.arg,
    stakeholders = c.metadata.stakeholders,
    signatories = c.metadata.signatories,
    choiceObservers = Set.empty[LfPartyId], // default archive choice has no choice observers
    choiceAuthorizers = None, // default (signatories + actingParties)
    children = ImmArray.empty[LfNodeId],
    exerciseResult = Some(LfValue.ValueNone),
    keyOpt = c.metadata.maybeKeyWithMaintainers,
    byKey = false,
    version = c.rawContractInstance.contractInstance.version,
  )

  private def writeContractsPurgedEvent(
      contracts: Seq[SerializableContract],
      updateId: UpdateId,
      repair: RepairRequest,
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contracts.map(toArchive)).toMap
    val update = Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = repair.timestamp.toLf,
        workflowId = None,
        preparationTime = repair.timestamp.toLf,
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
      updateId = updateId,
      synchronizerId = repair.synchronizer.psid.logical,
      repairCounter = repair.tryExactlyOneRepairCounter,
      recordTime = repair.timestamp,
      // no need to pass the contract infos since no create nodes are involved
      contractInfos = Map.empty,
    )
    // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
    repairIndexer.offer(update).map(_ => ())
  }

  private def publishUnassignedEvent(
      contracts: Seq[(SerializableContract, ReassignmentCounter)],
      repair: RepairRequest,
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    // Unassignments set the same source and target synchronizerIds since they are artificial
    // unassigns without an actual target synchronizer (this is used for purging a contract)
    val reassignmentId = ReassignmentId(
      ReassignmentTag.Source(repair.synchronizer.psid.logical),
      ReassignmentTag.Target(repair.synchronizer.psid.logical),
      unassignmentTs = repair.timestamp,
      contractIdCounters = contracts.map { case (c, reassignmentCounter) =>
        (c.contractId, reassignmentCounter)
      },
    )

    val unassigns = contracts.zipWithIndex
      .map { case ((c, reassignmentCounter), nodeId) =>
        Reassignment.Unassign(
          contractId = c.contractId,
          templateId = c.rawContractInstance.contractInstance.unversioned.template,
          packageName = c.rawContractInstance.contractInstance.unversioned.packageName,
          stakeholders = c.metadata.stakeholders,
          assignmentExclusivity = None,
          reassignmentCounter = reassignmentCounter.unwrap,
          nodeId = nodeId,
        )
      }

    NonEmpty
      .from(unassigns)
      .map { unassignsNE =>
        RepairReassignmentAccepted(
          workflowId = None,
          updateId = randomUpdateId(syncCrypto),
          reassignmentInfo = ReassignmentInfo(
            sourceSynchronizer = ReassignmentTag.Source(repair.synchronizer.psid.logical),
            targetSynchronizer = ReassignmentTag.Target(repair.synchronizer.psid.logical),
            submitter = None,
            reassignmentId = reassignmentId,
            isReassigningParticipant = false,
          ),
          reassignment = Reassignment.Batch(unassignsNE),
          repairCounter = repair.tryExactlyOneRepairCounter,
          recordTime = repair.timestamp,
          synchronizerId = repair.synchronizer.psid.logical,
        )
      }
      .fold(Future.unit)(repairIndexer.offer(_).map(_ => ()))
  }

  /** Allows to wait until clean sequencer index has progressed up to a certain timestamp */
  def awaitCleanSequencerTimestamp(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    def check(): FutureUnlessShutdown[Either[String, Unit]] =
      ledgerApiIndexer.value.ledgerApiStore.value
        .cleanSynchronizerIndex(synchronizerId)
        .map(SyncEphemeralStateFactory.lastSequencerTimestamp)
        .map { lastSequencerTimestamp =>
          if (lastSequencerTimestamp >= timestamp) {
            logger.debug(
              s"Clean sequencer index reached $lastSequencerTimestamp, clearing $timestamp"
            )
            Either.unit
          } else {
            val errMsg =
              s"Clean sequencer index is still at $lastSequencerTimestamp which is not yet $timestamp"
            logger.debug(errMsg)
            Left(errMsg)
          }
        }
    EitherT(
      retry
        .Pause(
          logger,
          this,
          retry.Forever,
          50.milliseconds,
          s"awaiting clean-head for=$synchronizerId at ts=$timestamp",
        )
        .unlessShutdown(
          check(),
          AllExceptionRetryPolicy,
        )
    )
  }
}

private object RepairService {

  private type MissingAssignment =
    (LfContractId, ReassignmentTag.Source[SynchronizerId], ReassignmentCounter, TimeOfChange)
  private type MissingPurge = (LfContractId, TimeOfChange)

  /** What needs to be done to purge a contract
    * @param purge
    *   Canton internal purge
    * @param assign
    *   Canton internal assign
    * @param upstream
    *   Data to generate the event that notifies the indexer
    */
  final case class PurgeOperations(
      purge: Option[MissingPurge],
      assign: Option[MissingAssignment],
      upstream: Option[(SerializableContract, ReassignmentCounter)],
  )

  private object PurgeOperations {
    def empty: PurgeOperations = PurgeOperations(None, None, None)
  }
}
