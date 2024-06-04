// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.value.{Identifier, Record}
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Salt, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.ledger.api.validation.StricterValueValidator as LedgerApiValueValidator
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.admin.repair.RepairService.ContractToAdd
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.util.DAMLe.ContractWithMetadata
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.participant.{ParticipantNodeParameters, RequestOffset}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{LfChoiceName, *}
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.{
  CursorPrehead,
  IndexedDomain,
  IndexedStringStore,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.time.Instant
import scala.Ordered.orderingToOrdered
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

/** Implements the repair commands.
  * Repair commands work only if the participant has disconnected from the affected domains.
  * Every individual repair commands is executed transactionally, i.e., either all its effects are applied or none.
  * This is achieved in the same way as for request processing:
  * <ol>
  *   <li>A request counter is allocated for the repair request (namely the clean request head) and
  *     marked as [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Pending]].
  *     The repair request's timestamp is the timestamp where processing starts again upon reconnection to the domain.</li>
  *   <li>All repair effects are persisted to the stores using the repair request counter.</li>
  *   <li>The clean request prehead is advanced to the repair request counter. This commits the changes.
  *     If multiple domains are involved, transactionality is ensured
  *     via the [[com.digitalasset.canton.resource.TransactionalStoreUpdate]] mechanism.</li>
  * </ol>
  * If anything goes wrong before advancing the clean request prehead,
  * the already persisted data will be cleaned up upon the next repair request or reconnection to the domain.
  *
  * @param executionQueue Sequential execution queue on which repair actions must be run.
  *                       This queue is shared with the CantonSyncService, which uses it for domain connections.
  *                       Sharing it ensures that we cannot connect to the domain while a repair action is running and vice versa.
  *                       It also ensure only one repair runs at a time. This ensures concurrent activity
  *                       among repair operations does not corrupt state.
  */
final class RepairService(
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiProvider,
    packageDependencyResolver: PackageDependencyResolver,
    damle: DAMLe,
    multiDomainEventLog: Eval[MultiDomainEventLog],
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    aliasManager: DomainAliasManager,
    parameters: ParticipantNodeParameters,
    threadsAvailableForWriting: PositiveInt,
    indexedStringStore: IndexedStringStore,
    isConnected: DomainId => Boolean,
    @VisibleForTesting
    private[canton] val executionQueue: SimpleExecutionQueue,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private type MissingContract = (WithTransactionId[SerializableContract], RequestCounter)
  private type MissingTransferIn = (LfContractId, SourceDomainId, TransferCounter, TimeOfChange)
  private type MissingAdd = (LfContractId, TransferCounter, TimeOfChange)
  private type MissingPurge = (LfContractId, TimeOfChange)

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private def aliasToUnconnectedDomainId(alias: DomainAlias): EitherT[Future, String, DomainId] =
    for {
      domainId <- EitherT.fromEither[Future](
        aliasManager.domainIdForAlias(alias).toRight(s"Could not find $alias")
      )
      _ <- domainNotConnected(domainId)
    } yield domainId
  private def domainNotConnected(domainId: DomainId): EitherT[Future, String, Unit] = EitherT.cond(
    !isConnected(domainId),
    (),
    s"Participant is still connected to domain $domainId",
  )

  private def contractToAdd(
      repairContract: RepairContract,
      ignoreAlreadyAdded: Boolean,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[SerializableContract],
  )(implicit traceContext: TraceContext): EitherT[Future, String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId

    def addContract(
        transferringFrom: Option[SourceDomainId]
    ): EitherT[Future, String, Option[ContractToAdd]] = Right(
      Option(
        ContractToAdd(
          repairContract.contract,
          repairContract.witnesses.map(_.toLf),
          repairContract.transferCounter,
          transferringFrom,
        )
      )
    ).toEitherT[Future]

    acsState match {
      case None => addContract(transferringFrom = None)

      case Some(ActiveContractStore.Active(_)) =>
        if (ignoreAlreadyAdded) {
          logger.debug(s"Skipping contract $contractId because it is already active")
          for {
            contractAlreadyThere <- EitherT.fromEither[Future](storedContract.toRight {
              s"Contract ${repairContract.contract.contractId} is active but is not found in the stores"
            })
            _ <- EitherTUtil.condUnitET[Future](
              contractAlreadyThere == repairContract.contract,
              log(
                s"Contract $contractId exists in domain, but does not match with contract being added. "
                  + s"Existing contract is $contractAlreadyThere while contract supposed to be added ${repairContract.contract}"
              ),
            )
          } yield None
        } else {
          EitherT.leftT {
            log(
              s"A contract with $contractId is already active. Set ignoreAlreadyAdded = true to skip active contracts."
            )
          }
        }
      case Some(ActiveContractStore.Archived) =>
        EitherT.leftT(
          log(
            s"Cannot add previously archived contract ${repairContract.contract.contractId} as archived contracts cannot become active."
          )
        )
      case Some(ActiveContractStore.Purged) => addContract(transferringFrom = None)
      case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
        log(
          s"Marking contract ${repairContract.contract.contractId} previously transferred-out to $targetDomain as " +
            s"transferred-in from $targetDomain (even though contract may have been transferred to yet another domain since)."
        ).discard

        val isTransferCounterIncreasing = repairContract.transferCounter > transferCounter

        if (isTransferCounterIncreasing) {
          addContract(transferringFrom = Option(SourceDomainId(targetDomain.unwrap)))
        } else {
          EitherT.leftT(
            log(
              s"The transfer counter ${repairContract.transferCounter} of the contract " +
                s"${repairContract.contract.contractId} needs to be strictly larger than the transfer counter " +
                s"$transferCounter at the time of the transfer-out."
            )
          )
        }

    }
  }

  /** Prepare contract for add, including re-computing metadata
    * @param domain Domain data of the repair request
    * @param repairContract Contract to be added
    * @param acsState If the contract is known, its status
    * @param storedContract If the contract already exist in the ContractStore, the stored copy
    */
  private def readRepairContractCurrentState(
      domain: RepairRequest.DomainData,
      repairContract: RepairContract,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[SerializableContract],
      ignoreAlreadyAdded: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[ContractToAdd]] =
    for {
      // Able to recompute contract signatories and stakeholders (and sanity check
      // repairContract metadata otherwise ignored matches real metadata)
      contractWithMetadata <- damle
        .contractWithMetadata(
          repairContract.contract.rawContractInstance.contractInstance,
          repairContract.contract.metadata.signatories,
          // There is currently no mechanism in place through which another service command can ask to abort the
          // engine computation for a previously sent contract. When therefore tell then engine to always continue.
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
        )
        .leftMap(e =>
          log(s"Failed to compute contract ${repairContract.contract.contractId} metadata: $e")
        )
      _ = if (repairContract.contract.metadata.signatories != contractWithMetadata.signatories) {
        logger.info(
          s"Contract ${repairContract.contract.contractId} metadata signatories ${repairContract.contract.metadata.signatories} differ from actual signatories ${contractWithMetadata.signatories}"
        )
      }
      _ = if (repairContract.contract.metadata.stakeholders != contractWithMetadata.stakeholders) {
        logger.info(
          s"Contract ${repairContract.contract.contractId} metadata stakeholders ${repairContract.contract.metadata.stakeholders} differ from actual stakeholders ${contractWithMetadata.stakeholders}"
        )
      }
      computedContract <- useComputedContractAndMetadata(
        repairContract.contract,
        contractWithMetadata,
      )
      contractToAdd <- contractToAdd(
        repairContract.copy(contract = computedContract),
        ignoreAlreadyAdded = ignoreAlreadyAdded,
        acsState = acsState,
        storedContract = storedContract,
      )
    } yield contractToAdd

  // The repair request gets inserted at the reprocessing starting point.
  // We use the prenextTimestamp such that a regular request is always the first request for a given timestamp.
  // This is needed for causality tracking, which cannot use a tie breaker on timestamps.
  //
  // If this repair request succeeds, it will advance the clean request prehead to this time of change.
  // That's why it is important that there are no inflight validation requests before the repair request.
  private def readDomainData(
      domainId: DomainId
  )(implicit traceContext: TraceContext): EitherT[Future, String, RepairRequest.DomainData] =
    for {
      obtainedPersistentState <- getPersistentState(domainId)
      (persistentState, domainAlias, indexedDomain) = obtainedPersistentState

      startingPoints <- EitherTUtil.fromFuture(
        SyncDomainEphemeralStateFactory.startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        ),
        t => log("Failed to compute starting points", t),
      )

      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(domainId)
        .toRight(s"No topology factory for domain $domainAlias")
        .toEitherT[Future]

      topologySnapshot = topologyFactory.createTopologySnapshot(
        startingPoints.processing.prenextTimestamp,
        packageDependencyResolver,
        preferCaching = true,
      )
      domainParameters <- OptionT(persistentState.parameterStore.lastParameters)
        .toRight(log(s"No static domains parameters found for $domainAlias"))
    } yield RepairRequest.DomainData(
      domainId,
      domainAlias,
      topologySnapshot,
      persistentState,
      domainParameters,
      startingPoints,
    )

  /** Participant repair utility for manually adding contracts to a domain in an offline fashion.
    *
    * @param domain             alias of domain to add contracts to. The domain needs to be configured, but disconnected
    *                           to prevent race conditions.
    * @param contracts          contracts to add. Relevant pieces of each contract: create-arguments (LfContractInst),
    *                           template-id (LfContractInst), contractId, ledgerCreateTime, salt (to be added to
    *                           SerializableContract), and witnesses, SerializableContract.metadata is only validated,
    *                           but otherwise ignored as stakeholder and signatories can be recomputed from contracts.
    * @param ignoreAlreadyAdded whether to ignore and skip over contracts already added/present in the domain. Setting
    *                           this to true (at least on retries) enables writing idempotent repair scripts.
    * @param ignoreStakeholderCheck do not check for stakeholder presence for the given parties
    * @param workflowIdPrefix   If present, each transaction generated for added contracts will have a workflow ID whose
    *                           prefix is the one set and the suffix is a sequential number and the number of transactions
    *                           generated as part of the addition (e.g. `import-foo-1-2`, `import-foo-2-2`)
    */
  def addContracts(
      domain: DomainAlias,
      contracts: Seq[RepairContract],
      ignoreAlreadyAdded: Boolean,
      ignoreStakeholderCheck: Boolean,
      workflowIdPrefix: Option[String] = None,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Adding ${contracts.length} contracts to domain $domain with ignoreAlreadyAdded=$ignoreAlreadyAdded and ignoreStakeholderCheck=$ignoreStakeholderCheck"
    )
    if (contracts.isEmpty) {
      Either.right(logger.info("No contracts to add specified"))
    } else {
      lockAndAwaitDomainAlias(
        "repair.add",
        domainId => {
          for {
            domain <- readDomainData(domainId)

            contractStates <- readContractAcsStates(
              domain.persistentState,
              contracts.map(_.contract.contractId),
            )

            storedContracts <- fromFuture(
              domain.persistentState.contractStore.lookupManyUncached(
                contracts.map(_.contract.contractId)
              ),
              "Unable to lookup contracts in contract store",
            ).map { contracts =>
              contracts.view.flatMap(_.map(c => c.contractId -> c.contract)).toMap
            }

            filteredContracts <- contracts.zip(contractStates).parTraverseFilter {
              case (contract, acsState) =>
                readRepairContractCurrentState(
                  domain,
                  repairContract = contract,
                  acsState = acsState,
                  storedContract = storedContracts.get(contract.contract.contractId),
                  ignoreAlreadyAdded = ignoreAlreadyAdded,
                )
            }

            contractsByCreation = filteredContracts
              .groupBy(_.contract.ledgerCreateTime)
              .toList
              .sortBy { case (ledgerCreateTime, _) => ledgerCreateTime }

            _ <- PositiveInt
              .create(contractsByCreation.size)
              .fold(
                _ => EitherT.rightT[Future, String](logger.info("No contract needs to be added")),
                groupCount => {
                  val workflowIds = workflowIdsFromPrefix(workflowIdPrefix, groupCount)
                  for {
                    repair <- initRepairRequestAndVerifyPreconditions(
                      domain = domain,
                      requestCountersToAllocate = groupCount,
                    )

                    hostedWitnesses <- EitherT.right(
                      hostsParties(
                        repair.domain.topologySnapshot,
                        filteredContracts.flatMap(_.witnesses).toSet,
                        participantId,
                      )
                    )

                    _ <- addContractsCheck(
                      repair,
                      hostedWitnesses = hostedWitnesses,
                      ignoreStakeholderCheck = ignoreStakeholderCheck,
                      filteredContracts,
                    )

                    contractsToAdd = repair.timesOfChange.zip(contractsByCreation)

                    _ = logger.debug(s"Publishing ${filteredContracts.size} added contracts")

                    contractsWithTimeOfChange = contractsToAdd.flatMap { case (toc, (_, cs)) =>
                      cs.map(_ -> toc)
                    }

                    _ <- persistAddContracts(
                      repair,
                      contractsToAdd = contractsWithTimeOfChange,
                      storedContracts = storedContracts,
                    )

                    // Publish added contracts upstream as created via the ledger api.
                    _ <- EitherT.right(
                      writeContractsAddedEvents(
                        repair,
                        hostedWitnesses,
                        contractsToAdd,
                        workflowIds,
                      )
                    )

                    // If all has gone well, bump the clean head, effectively committing the changes to the domain.
                    // TODO(#19140) This should be done once all the requests succeed
                    _ <- commitRepairs(repair)

                  } yield ()
                },
              )
          } yield ()
        },
        domain,
      )
    }
  }

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

  /** Participant repair utility for manually purging (archiving) contracts in an offline fashion.
    *
    * @param domain              alias of domain to purge contracts from. The domain needs to be configured, but
    *                            disconnected to prevent race conditions.
    * @param contractIds         lf contract ids of contracts to purge
    * @param ignoreAlreadyPurged whether to ignore already purged contracts.
    */
  def purgeContracts(
      domain: DomainAlias,
      contractIds: NonEmpty[Seq[LfContractId]],
      ignoreAlreadyPurged: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Purging ${contractIds.length} contracts from $domain with ignoreAlreadyPurged=$ignoreAlreadyPurged"
    )
    lockAndAwaitDomainAlias(
      "repair.purge",
      domainId => {
        for {
          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          contractStates <- readContractAcsStates(
            repair.domain.persistentState,
            contractIds,
          )

          storedContracts <- fromFuture(
            repair.domain.persistentState.contractStore.lookupManyUncached(contractIds),
            "Unable to lookup contracts in contract store",
          ).map { contracts =>
            contracts.view.flatMap(_.map(c => c.contractId -> c.contract)).toMap
          }

          operationsE = contractIds
            .zip(contractStates)
            .foldMapM { case (cid, acsStatus) =>
              val storedContract = storedContracts.get(cid)
              computePurgeOperations(repair, ignoreAlreadyPurged)(cid, acsStatus, storedContract)
                .map { case (missingPurge, missingTransferIn) =>
                  (storedContract.toList, missingPurge, missingTransferIn)
                }
            }
          operations <- EitherT.fromEither[Future](operationsE)

          (contractsToPublishUpstream, missingPurges, missingTransferIns) = operations

          // Update the stores
          _ <- repair.domain.persistentState.activeContractStore
            .purgeContracts(missingPurges)
            .toEitherTWithNonaborts
            .leftMap(e =>
              log(s"Failed to purge contractd $missingTransferIns in ActiveContractStore: $e")
            )

          _ <- repair.domain.persistentState.activeContractStore
            .transferInContracts(missingTransferIns)
            .toEitherTWithNonaborts
            .leftMap(e =>
              log(s"Failed to transfer-in contractd $missingTransferIns in ActiveContractStore: $e")
            )

          // Publish purged contracts upstream as archived via the ledger api.
          _ <- EitherT.right(writeContractsPurgedEvent(contractsToPublishUpstream, Nil, repair))

          // If all has gone well, bump the clean head, effectively committing the changes to the domain.
          _ <- commitRepairs(repair)

        } yield ()
      },
      domain,
    )
  }

  /** Participant repair utility to manually change assignation of contracts
    * from a source domain to a target domain in an offline fashion.
    *
    * @param contractIds    IDs of contracts that will change assignation
    * @param sourceDomain   alias of source domain the contracts are assigned to before the change
    * @param targetDomain   alias of target domain the contracts will be assigned to after the change
    * @param skipInactive   whether to only change assignment of contracts that are active in the source domain
    * @param batchSize      how big the batches should be used during the change assignation process
    */
  def changeAssignationAwait(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Change assignation request for ${contractIds.length} contracts from $sourceDomain to $targetDomain with skipInactive=$skipInactive"
    )
    lockAndAwaitDomainPair(
      "repair.change_assignation",
      (sourceDomainId, targetDomainId) => {
        for {
          _ <- changeAssignation(
            contractIds,
            sourceDomainId,
            targetDomainId,
            skipInactive,
            batchSize,
          )
        } yield ()
      },
      (sourceDomain, targetDomain),
    )
  }

  /** Change the assignation of a contract from one domain to another
    *
    * This function here allows us to manually insert a transfer out / in into the respective
    * journals in order to move a contract from one domain to another. The procedure will result in
    * a consistent state if and only if all the counter parties run the same command. Failure to do so,
    * will results in participants reporting errors and possibly break.
    *
    * @param skipInactive if true, then the migration will skip contracts in the contractId list that are inactive
    */
  def changeAssignation(
      contractIds: Seq[LfContractId],
      sourceDomainId: DomainId,
      targetDomainId: DomainId,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    PositiveInt
      .create(contractIds.size)
      .map { numberOfContracts =>
        for {
          _ <- EitherTUtil.condUnitET[Future](
            sourceDomainId != targetDomainId,
            "Source must differ from target domain!",
          )
          repairSource <- initRepairRequestAndVerifyPreconditions(sourceDomainId, numberOfContracts)
          repairTarget <- initRepairRequestAndVerifyPreconditions(targetDomainId, numberOfContracts)

          changeAssignationData =
            contractIds
              .zip(repairSource.timesOfChange)
              .zip(repairTarget.timesOfChange)
              .map { case ((contractId, sourceToc), targetToc) =>
                ChangeAssignation.Data(contractId, sourceToc, targetToc)
              }

          _ <- changeAssignationBatched(
            changeAssignationData,
            repairSource,
            repairTarget,
            skipInactive,
            batchSize,
          )

          // If all has gone well, bump the clean head to both domains transactionally
          _ <- commitRepairs(repairTarget, repairSource)
        } yield ()
      }
      .getOrElse(EitherT.rightT(()))

  def ignoreEvents(domain: DomainId, from: SequencerCounter, to: SequencerCounter, force: Boolean)(
      implicit traceContext: TraceContext
  ): Either[String, Unit] = {
    logger.info(s"Ignoring sequenced events from $from to $to (force = $force).")
    lockAndAwaitDomainId(
      "repair.skip_messages",
      for {
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(
          _.ignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
      domain,
    )
  }

  private def performIfRangeSuitableForIgnoreOperations[T](
      domain: DomainId,
      from: SequencerCounter,
      force: Boolean,
  )(
      action: SequencedEventStore => EitherT[Future, String, T]
  )(implicit traceContext: TraceContext): EitherT[Future, String, T] =
    for {
      persistentState <- EitherT.fromEither[Future](lookUpDomainPersistence(domain, domain.show))
      indexedDomain <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domain))
      startingPoints <- EitherT.right(
        SyncDomainEphemeralStateFactory.startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        )
      )

      _ <- EitherTUtil
        .condUnitET[Future](
          force || startingPoints.rewoundSequencerCounterPrehead.forall(_.counter <= from),
          show"Unable to modify events between $from (inclusive) and ${startingPoints.rewoundSequencerCounterPrehead.showValue} (exclusive), " +
            """as they won't be read from the sequencer client. Enable "force" to modify them nevertheless.""",
        )

      _ <- EitherTUtil
        .condUnitET[Future](
          force || startingPoints.processing.nextSequencerCounter <= from,
          show"Unable to modify events between $from (inclusive) and ${startingPoints.processing.nextSequencerCounter} (exclusive), " +
            """as they have already been processed. Enable "force" to modify them nevertheless.""",
        )
      res <- action(persistentState.sequencedEventStore)
    } yield res

  def unignoreEvents(
      domain: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(s"Unignoring sequenced events from $from to $to (force = $force).")
    lockAndAwaitDomainId(
      "repair.unskip_messages",
      for {
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(sequencedEventStore =>
          sequencedEventStore.unignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
      domain,
    )
  }

  private def useComputedContractAndMetadata(
      inputContract: SerializableContract,
      computed: ContractWithMetadata,
  )(implicit traceContext: TraceContext): EitherT[Future, String, SerializableContract] =
    EitherT.fromEither[Future](
      for {
        rawContractInstance <- SerializableRawContractInstance
          .create(computed.instance)
          .leftMap(err =>
            log(s"Failed to serialize contract ${inputContract.contractId}: ${err.errorMessage}")
          )
      } yield inputContract.copy(
        metadata = computed.metadataWithGlobalKey,
        rawContractInstance = rawContractInstance,
      )
    )

  /** Checks that the contracts can be added (packages known, stakeholders hosted, ...)
    * @param hostedWitnesses All parties that are a witness on one of the contract
    *                        and are hosted locally
    */
  private def addContractsCheck(
      repair: RepairRequest,
      hostedWitnesses: Set[LfPartyId],
      ignoreStakeholderCheck: Boolean,
      contracts: Seq[ContractToAdd],
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      // All referenced templates known and vetted
      _packagesVetted <- contracts
        .map(
          _.contract.rawContractInstance.contractInstance.unversioned.template.packageId
        )
        .distinct
        .parTraverse_(packageKnown)

      _ <- contracts.parTraverse_(
        addContractChecks(repair, hostedWitnesses, ignoreStakeholderCheck = ignoreStakeholderCheck)
      )
    } yield ()
  }

  /** Checks that one contract can be added (stakeholders hosted, ...)
    * @param hostedParties Relevant locally hosted parties
    */
  private def addContractChecks(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      ignoreStakeholderCheck: Boolean,
  )(
      contractToAdd: ContractToAdd
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    val topologySnapshot = repair.domain.topologySnapshot
    val contract = contractToAdd.contract
    val contractId = contractToAdd.cid
    for {
      _warnOnEmptyMaintainers <- EitherT.cond[Future](
        !contract.metadata.maybeKeyWithMaintainers.exists(_.maintainers.isEmpty),
        (),
        log(s"Contract $contractId has key without maintainers."),
      )

      // Witnesses all known locally.
      missingWitnesses = contractToAdd.witnesses.diff(hostedParties)
      _witnessesKnownLocallyE = NonEmpty.from(missingWitnesses).toLeft(()).leftMap {
        missingWitnessesNE =>
          log(
            s"Witnesses $missingWitnessesNE not active on domain ${repair.domain.alias} and local participant"
          )
      }

      _witnessesKnownLocally <- EitherT.fromEither[Future](_witnessesKnownLocallyE)

      _ <-
        if (ignoreStakeholderCheck) EitherT.rightT[Future, String](())
        else
          for {
            // At least one stakeholder is hosted locally if no witnesses are defined
            _localStakeholderOrWitnesses <- EitherT(
              hostsParties(topologySnapshot, contract.metadata.stakeholders, participantId).map {
                localStakeholders =>
                  EitherUtil.condUnitE(
                    contractToAdd.witnesses.nonEmpty || localStakeholders.nonEmpty,
                    log(
                      s"Contract ${contract.contractId} has no local stakeholders ${contract.metadata.stakeholders} and no witnesses defined"
                    ),
                  )
              }
            )
            // All stakeholders exist on the domain
            _ <- topologySnapshot
              .allHaveActiveParticipants(contract.metadata.stakeholders)
              .leftMap { missingStakeholders =>
                log(
                  s"Domain ${repair.domain.alias} missing stakeholders $missingStakeholders of contract ${contract.contractId}"
                )
              }
          } yield ()

      // All witnesses exist on the domain
      _ <- topologySnapshot.allHaveActiveParticipants(contractToAdd.witnesses).leftMap {
        missingWitnesses =>
          log(
            s"Domain ${repair.domain.alias} missing witnesses $missingWitnesses of contract ${contract.contractId}"
          )
      }
    } yield ()
  }

  private def fromFuture[T](f: Future[T], errorMessage: => String)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, T] =
    EitherTUtil.fromFuture(
      f,
      t => log(s"$errorMessage: ${ErrorUtil.messageWithStacktrace(t)}"),
    )

  /** Actual persistence work
    * @param repair           Repair request
    * @param contractsToAdd   Contracts to be added
    * @param storedContracts  Contracts that already exists in the store
    */
  private def persistAddContracts(
      repair: RepairRequest,
      contractsToAdd: Seq[(ContractToAdd, TimeOfChange)],
      storedContracts: Map[LfContractId, SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    for {
      // We compute first which changes we need to persist
      missingContracts <- contractsToAdd
        .parTraverseFilter[EitherT[Future, String, *], MissingContract] {
          case (contractToAdd, toc) =>
            storedContracts.get(contractToAdd.cid) match {
              case None =>
                EitherT.pure[Future, String](
                  Some((WithTransactionId(contractToAdd.contract, repair.transactionId), toc.rc))
                )

              case Some(storedContract) =>
                EitherTUtil
                  .condUnitET[Future](
                    storedContract == contractToAdd.contract,
                    s"Contract ${contractToAdd.cid} already exists in the contract store, but differs from contract to be created. Contract to be created $contractToAdd versus existing contract $storedContract.",
                  )
                  .map(_ => Option.empty[MissingContract])
            }
        }

      (missingTransferIns, missingAdds) = contractsToAdd.foldLeft(
        (Seq.empty[MissingTransferIn], Seq.empty[MissingAdd])
      ) { case ((missingTransferIns, missingAdds), (contract, toc)) =>
        contract.transferringFrom match {
          case Some(sourceDomainId) =>
            val newTransferIn = (contract.cid, sourceDomainId, contract.transferCounter, toc)
            (newTransferIn +: missingTransferIns, missingAdds)

          case None =>
            val newAdd = (contract.cid, contract.transferCounter, toc)
            (missingTransferIns, newAdd +: missingAdds)
        }
      }

      // Now, we update the stores
      _ <- fromFuture(
        repair.domain.persistentState.contractStore.storeCreatedContracts(missingContracts),
        "Unable to store missing contracts",
      )

      _ <- repair.domain.persistentState.activeContractStore
        .markContractsAdded(missingAdds)
        .toEitherTWithNonaborts
        .leftMap(e =>
          log(
            s"Failed to add contracts ${missingAdds.map { case (cid, _, _) => cid }} in ActiveContractStore: $e"
          )
        )

      _ <- repair.domain.persistentState.activeContractStore
        .transferInContracts(missingTransferIns)
        .toEitherTWithNonaborts
        .leftMap(e =>
          log(
            s"Failed to transfer-in ${missingTransferIns.map { case (cid, _, _, _) => cid }} in ActiveContractStore: $e"
          )
        )

    } yield ()
  }

  /** For the given contract, returns the operations (purge, transfer-in to perform
    * @param acsStatus Status of the contract
    * @param storedContractO Instance of the contract
    * @return
    */
  private def computePurgeOperations(repair: RepairRequest, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId,
      acsStatus: Option[ActiveContractStore.Status],
      storedContractO: Option[SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): Either[String, (Seq[MissingPurge], Seq[MissingTransferIn])] = {
    def ignoreOrError(reason: String): Either[String, (Seq[MissingPurge], Seq[MissingTransferIn])] =
      Either.cond(
        ignoreAlreadyPurged,
        (Nil, Nil),
        log(
          s"Contract $cid cannot be purged: $reason. Set ignoreAlreadyPurged = true to skip non-existing contracts."
        ),
      )

    val toc = repair.tryExactlyOneTimeOfChange

    // Not checking that the participant hosts a stakeholder as we might be cleaning up contracts
    // on behalf of stakeholders no longer around.
    acsStatus match {
      case None => ignoreOrError("unknown contract")
      case Some(ActiveContractStore.Active(_)) =>
        for {
          _contract <- Either
            .fromOption(
              storedContractO,
              log(show"Active contract $cid not found in contract store"),
            )
        } yield {
          (Seq[MissingPurge]((cid, toc)), Seq.empty[MissingTransferIn])
        }
      case Some(ActiveContractStore.Archived) => ignoreOrError("archived contract")
      case Some(ActiveContractStore.Purged) => ignoreOrError("purged contract")
      case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
        log(
          s"Purging contract $cid previously marked as transferred away to $targetDomain. " +
            s"Marking contract as transferred-in from $targetDomain (even though contract may have since been transferred to yet another domain) and subsequently as archived."
        ).discard

        transferCounter.increment.map { newTransferCounter =>
          (
            Seq[MissingPurge]((cid, toc)),
            Seq[MissingTransferIn](
              (
                cid,
                SourceDomainId(targetDomain.unwrap),
                newTransferCounter,
                toc,
              )
            ),
          )
        }
    }
  }

  private def changeAssignationBatched(
      contractIds: Seq[ChangeAssignation.Data[LfContractId]],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    MonadUtil
      .batchedSequentialTraverse(threadsAvailableForWriting * PositiveInt.two, batchSize)(
        contractIds
      )(
        ChangeAssignation(
          _,
          repairSource,
          repairTarget,
          skipInactive,
          participantId,
          syncCrypto,
          loggerFactory,
        )
          .map(_ => Seq[Unit]())
      )
      .map(_ => ())

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
      hostedWitnesses: Seq[LfPartyId],
      repair: RepairRequest,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val transactionId = repair.transactionId.tryAsLedgerTransactionId
    val offset = RequestOffset(repair.timestamp, repair.tryExactlyOneRequestCounter)
    val event =
      TimestampedEvent(
        LedgerSyncEvent.ContractsPurged(
          transactionId = transactionId,
          contracts = contracts.map(toArchive),
          domainId = repair.domain.id,
          recordTime = repair.timestamp.toLf,
          hostedWitnesses = hostedWitnesses,
        ),
        offset,
        None,
      )

    repair.domain.persistentState.eventLog.insertsUnlessEventIdClash(Seq(event)).map { outcomes =>
      if (outcomes.headOption.exists(_.isLeft)) {
        logger.info(
          show"Skipping duplicate publication of event at offset $offset with transaction id $transactionId."
        )
      }
    }
  }

  private def prepareAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      requestCounter: RequestCounter,
      ledgerCreateTime: LedgerCreateTime,
      contractsAdded: Seq[ContractToAdd],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): TimestampedEvent = {
    val transactionId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId
    val offset = RequestOffset(repair.timestamp, requestCounter)
    val contractMetadata = contractsAdded.view
      .map(c => c.contract.contractId -> c.driverMetadata(repair.domain.parameters.protocolVersion))
      .toMap

    TimestampedEvent(
      LedgerSyncEvent.ContractsAdded(
        transactionId = transactionId,
        contracts = contractsAdded.map(_.contract.toLf),
        domainId = repair.domain.id,
        recordTime = repair.timestamp.toLf,
        ledgerTime = ledgerCreateTime.toLf,
        hostedWitnesses = contractsAdded.flatMap(_.witnesses.intersect(hostedParties)),
        contractMetadata = contractMetadata,
        workflowId = workflowIdProvider(),
      ),
      offset,
      None,
    )
  }

  private def writeContractsAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      contractsAdded: Seq[(TimeOfChange, (LedgerCreateTime, Seq[ContractToAdd]))],
      workflowIds: Iterator[Option[LfWorkflowId]],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val events = contractsAdded.map { case (timeOfChange, (timestamp, contractsToAdd)) =>
      prepareAddedEvents(
        repair,
        hostedParties,
        timeOfChange.rc,
        timestamp,
        contractsToAdd,
        () => workflowIds.next(),
      )
    }

    repair.domain.persistentState.eventLog.insert(events)
  }

  private def packageKnown(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      packageDescription <- EitherTUtil.fromFuture(
        packageDependencyResolver.getPackageDescription(lfPackageId),
        t => log(s"Failed to look up package $lfPackageId", t),
      )
      _packageVetted <- EitherTUtil
        .condUnitET[Future](
          packageDescription.nonEmpty,
          log(s"Failed to locate package $lfPackageId"),
        )
    } yield ()
  }

  /** Allows to wait until clean head has progressed up to a certain timestamp */
  def awaitCleanHeadForTimestamp(
      domainId: DomainId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    def check(
        persistentState: SyncDomainPersistentState,
        indexedDomain: IndexedDomain,
    ): Future[Either[String, Unit]] = {
      SyncDomainEphemeralStateFactory
        .startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        )
        .map { startingPoints =>
          if (startingPoints.processing.prenextTimestamp >= timestamp) {
            logger.debug(
              s"Clean head reached ${startingPoints.processing.prenextTimestamp}, clearing ${timestamp}"
            )
            Right(())
          } else {
            logger.debug(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet ${timestamp}"
            )
            Left(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet ${timestamp}"
            )
          }
        }
    }
    getPersistentState(domainId)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap { case (persistentState, _, indexedDomain) =>
        EitherT(
          retry
            .Pause(
              logger,
              this,
              retry.Forever,
              50.milliseconds,
              s"awaiting clean-head for=${domainId} at ts=${timestamp}",
            )
            .unlessShutdown(
              FutureUnlessShutdown.outcomeF(check(persistentState, indexedDomain)),
              AllExnRetryable,
            )
        )
      }
  }

  private def requestCounterSequence(
      fromInclusive: RequestCounter,
      length: PositiveInt,
  ): Either[String, NonEmpty[Seq[RequestCounter]]] =
    for {
      rcs <- Seq
        .iterate(fromInclusive.asRight[String], length.value)(_.flatMap(_.increment))
        .sequence
      ne <- NonEmpty.from(rcs).toRight("generated an empty collection with PositiveInt length")
    } yield ne

  private def getPersistentState(
      domainId: DomainId
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, (SyncDomainPersistentState, String, IndexedDomain)] = {
    val domainAlias = aliasManager.aliasForDomainId(domainId).fold(domainId.filterString)(_.unwrap)
    for {
      persistentState <- EitherT.fromEither[Future](
        lookUpDomainPersistence(domainId, s"domain ${domainAlias}")
      )
      indexedDomain <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
    } yield (persistentState, domainAlias, indexedDomain)
  }

  /** Repair commands are inserted where processing starts again upon reconnection.
    *
    * @param domainId The ID of the domain for which the request is valid
    * @param requestCountersToAllocate The number of request counters to allocate in order to fulfill the request
    */
  private def initRepairRequestAndVerifyPreconditions(
      domainId: DomainId,
      requestCountersToAllocate: PositiveInt = PositiveInt.one,
  )(implicit traceContext: TraceContext): EitherT[Future, String, RepairRequest] =
    for {
      domainData <- readDomainData(domainId)
      repairRequest <- initRepairRequestAndVerifyPreconditions(
        domainData,
        requestCountersToAllocate,
      )
    } yield repairRequest

  private def initRepairRequestAndVerifyPreconditions(
      domain: RepairRequest.DomainData,
      requestCountersToAllocate: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, RepairRequest] = {
    val rtRepair = RecordTime.fromTimeOfChange(
      TimeOfChange(
        domain.startingPoints.processing.nextRequestCounter,
        domain.startingPoints.processing.prenextTimestamp,
      )
    )
    logger
      .debug(
        s"Starting repair request on (${domain.persistentState}, ${domain.alias}) at $rtRepair."
      )
    for {
      _ <- EitherT.cond[Future](
        domain.startingPoints.processingAfterPublished,
        (),
        log(
          s"""Cannot apply a repair command as events have been published up to
             |${domain.startingPoints.lastPublishedRequestOffset} offset inclusive
             |and the repair command would be assigned the offset ${domain.startingPoints.processing.nextRequestCounter}.
             |Reconnect to the domain to reprocess the inflight validation requests and retry repair afterwards.""".stripMargin
        ),
      )
      _ <- EitherTUtil.fromFuture(
        SyncDomainEphemeralStateFactory
          .cleanupPersistentState(domain.persistentState, domain.startingPoints),
        t => log(s"Failed to clean up the persistent state", t),
      )
      incrementalAcsSnapshotWatermark <- EitherTUtil.fromFuture(
        domain.persistentState.acsCommitmentStore.runningCommitments.watermark,
        _ => log(s"Failed to obtain watermark from incremental ACS snapshot"),
      )
      _ <- EitherT.cond[Future](
        rtRepair > incrementalAcsSnapshotWatermark,
        (),
        log(
          s"""Cannot apply a repair command as the incremental acs snapshot is already at $incrementalAcsSnapshotWatermark
             |and the repair command would be assigned a record time of $rtRepair.
             |Reconnect to the domain to reprocess inflight validation requests and retry repair afterwards.""".stripMargin
        ),
      )
      // Make sure that the topology state for the repair timestamp is available.
      // The topology manager does not maintain its own watermark of processed events,
      // so we conservatively approximate this via the clean sequencer counter prehead.
      _ <- domain.startingPoints.rewoundSequencerCounterPrehead match {
        case None =>
          EitherT.cond[Future](
            // Check that this is an empty domain.
            // We don't check the request counter because there may already have been earlier repairs on this domain.
            domain.startingPoints.processing.prenextTimestamp == RepairService.RepairTimestampOnEmptyDomain,
            (),
            log(
              s"""Cannot apply a repair command to ${domain.id} as no events have been completely processed on this non-empty domain.
                 |Reconnect to the domain to initialize the identity management and retry repair afterwards.""".stripMargin
            ),
          )
        case Some(CursorPrehead(_, tsClean)) =>
          EitherT.cond[Future](
            tsClean <= domain.startingPoints.processing.prenextTimestamp,
            (),
            log(
              s"""Cannot apply a repair command at ${domain.startingPoints.processing.prenextTimestamp} to ${domain.id}
                 |as event processing has completed only up to $tsClean.
                 |Reconnect to the domain to process the locally stored events and retry repair afterwards.""".stripMargin
            ),
          )
      }
      requestCounters <- EitherT.fromEither[Future](
        requestCounterSequence(
          domain.startingPoints.processing.nextRequestCounter,
          requestCountersToAllocate,
        )
      )
      repair = RepairRequest(
        domain,
        randomTransactionId(syncCrypto),
        requestCounters,
        RepairContext.tryFromTraceContext,
      )

      // Mark the repair request as pending in the request journal store
      _ <- EitherTUtil.fromFuture(
        repair.requestData.parTraverse_(domain.persistentState.requestJournalStore.insert),
        t => log("Failed to insert repair request", t),
      )

    } yield repair
  }

  private def markClean(
      repair: RepairRequest
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    repair.requestCounters.forgetNE
      .parTraverse_(
        repair.domain.persistentState.requestJournalStore.replace(
          _,
          repair.timestamp,
          RequestState.Clean,
          Some(repair.timestamp),
        )
      )
      .leftMap(t => log(s"Failed to update request journal store on ${repair.domain.alias}: $t"))
  }

  private def commitRepairs(
      repairs: RepairRequest*
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _ <- repairs.parTraverse_(markClean)
      _ <- EitherTUtil.fromFuture(
        TransactionalStoreUpdate.execute {
          repairs.map { repair =>
            repair.domain.persistentState.requestJournalStore
              .advancePreheadCleanToTransactionalUpdate(
                CursorPrehead(repair.requestCounters.last1, repair.timestamp)
              )
          }
        },
        t => log("Failed to advance clean request preheads", t),
      )
    } yield ()

  /** Read the ACS state for each contract in cids
    * @return The list of states or an error if one of the states cannot be read.
    * Note that the returned Seq has same ordering and cardinality of cids
    */
  private def readContractAcsStates(
      persistentState: SyncDomainPersistentState,
      cids: Seq[LfContractId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Seq[Option[ActiveContractStore.Status]]] =
    EitherTUtil
      .fromFuture(
        persistentState.activeContractStore.fetchStates(cids),
        e => log(s"Failed to look up contracts status in ActiveContractStore", e),
      )
      .map { states =>
        cids.map(cid => states.get(cid).map(_.status))
      }

  // Looks up domain persistence erroring if domain is based on in-memory persistence for which repair is not supported.
  private def lookUpDomainPersistence(domainId: DomainId, domainDescription: String)(implicit
      traceContext: TraceContext
  ): Either[String, SyncDomainPersistentState] =
    for {
      dp <- syncDomainPersistentStateManager
        .get(domainId)
        .toRight(log(s"Could not find $domainDescription"))
      _ <- Either.cond(
        !dp.isMemory,
        (),
        log(
          s"$domainDescription is in memory which is not supported by repair. Use db persistence."
        ),
      )
    } yield dp

  private def lockAndAwait[A, B](
      description: String,
      code: => EitherT[Future, String, B],
      domainIds: EitherT[Future, String, Seq[DomainId]],
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    logger.info(s"Queuing $description")
    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.await(description)(
      executionQueue
        .executeE(
          domainIds
            // Ensure we're not connected to any of the domains before running the code
            .flatMap(_.parTraverse_(domainNotConnected))
            .flatMap(_ => code),
          description,
        )
        .value
        .onShutdown(Left(s"$description aborted due to shutdown"))
    )
  }

  private def lockAndAwaitDomainId[B](
      description: String,
      code: => EitherT[Future, String, B],
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    lockAndAwait(
      description,
      code,
      EitherT.pure(Seq(domainId)),
    )
  }

  private def lockAndAwaitDomainAlias[B](
      description: String,
      code: DomainId => EitherT[Future, String, B],
      domainAlias: DomainAlias,
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    val domainId = EitherT.fromEither[Future](
      aliasManager.domainIdForAlias(domainAlias).toRight(s"Could not find $domainAlias")
    )

    lockAndAwait(
      description,
      domainId.flatMap(code),
      domainId.map(Seq(_)),
    )
  }

  private def lockAndAwaitDomainPair[B](
      description: String,
      code: (DomainId, DomainId) => EitherT[Future, String, B],
      domainAliases: (DomainAlias, DomainAlias),
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    val domainIds = (
      aliasToUnconnectedDomainId(domainAliases._1),
      aliasToUnconnectedDomainId(domainAliases._2),
    ).tupled
    lockAndAwait[(DomainId, DomainId), B](
      description,
      domainIds.flatMap(Function.tupled(code)),
      domainIds.map({ case (d1, d2) => Seq(d1, d2) }),
    )
  }

  private def log(message: String, cause: Throwable)(implicit traceContext: TraceContext): String =
    log(s"$message: ${ErrorUtil.messageWithStacktrace(cause)}")

  private def log(message: String)(implicit traceContext: TraceContext): String = {
    // consider errors user errors and log them on the server side as info:
    logger.info(message)
    message
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)

}

object RepairService {

  /** The timestamp to be used for a repair request on a domain without requests */
  val RepairTimestampOnEmptyDomain: CantonTimestamp = CantonTimestamp.MinValue

  object ContractConverter extends HasLoggerName {

    def contractDataToInstance(
        templateId: Identifier,
        packageName: LfPackageName,
        createArguments: Record,
        signatories: Set[String],
        observers: Set[String],
        lfContractId: LfContractId,
        ledgerTime: Instant,
        contractSalt: Option[Salt],
    )(implicit namedLoggingContext: NamedLoggingContext): Either[String, SerializableContract] = {
      for {
        template <- LedgerApiValueValidator.validateIdentifier(templateId).leftMap(_.getMessage)

        argsValue <- LedgerApiValueValidator
          .validateRecord(createArguments)
          .leftMap(e => s"Failed to validate arguments: ${e}")

        argsVersionedValue = LfVersioned(
          protocol.DummyTransactionVersion, // Version is ignored by daml engine upon RepairService.addContract
          argsValue,
        )

        lfContractInst = LfContractInst(
          packageName = packageName,
          template = template,
          packageVersion = None,
          arg = argsVersionedValue,
        )

        serializableRawContractInst <- SerializableRawContractInstance
          .create(lfContractInst)
          .leftMap(_.errorMessage)

        signatoriesAsParties <- signatories.toList.traverse(LfPartyId.fromString).map(_.toSet)
        observersAsParties <- observers.toList.traverse(LfPartyId.fromString).map(_.toSet)

        time <- CantonTimestamp.fromInstant(ledgerTime)
      } yield SerializableContract(
        contractId = lfContractId,
        rawContractInstance = serializableRawContractInst,
        // TODO(#13870): Calculate contract keys from the serializable contract
        metadata = checked(
          ContractMetadata
            .tryCreate(signatoriesAsParties, signatoriesAsParties ++ observersAsParties, None)
        ),
        ledgerCreateTime = LedgerCreateTime(time),
        contractSalt = contractSalt,
      )
    }

    def contractInstanceToData(
        contract: SerializableContract
    ): Either[
      String,
      (
          Identifier,
          LfPackageName,
          Record,
          Set[String],
          Set[String],
          LfContractId,
          Option[Salt],
          LedgerCreateTime,
      ),
    ] = {
      val contractInstance = contract.rawContractInstance.contractInstance
      LfEngineToApi
        .lfValueToApiRecord(verbose = true, contractInstance.unversioned.arg)
        .bimap(
          e =>
            s"Failed to convert contract instance to data due to issue with create-arguments: ${e}",
          record => {
            val signatories = contract.metadata.signatories.map(_.toString)
            val stakeholders = contract.metadata.stakeholders.map(_.toString)
            (
              LfEngineToApi.toApiIdentifier(contractInstance.unversioned.template),
              contractInstance.unversioned.packageName,
              record,
              signatories,
              stakeholders -- signatories,
              contract.contractId,
              contract.contractSalt,
              contract.ledgerCreateTime,
            )
          },
        )
    }
  }

  private final case class ContractToAdd(
      contract: SerializableContract,
      witnesses: Set[LfPartyId],
      transferCounter: TransferCounter,
      transferringFrom: Option[SourceDomainId],
  ) {
    def cid: LfContractId = contract.contractId

    def driverMetadata(protocolVersion: ProtocolVersion): Bytes =
      contract.contractSalt
        .map(DriverContractMetadata(_).toLfBytes(protocolVersion))
        .getOrElse(Bytes.Empty)
  }
}
