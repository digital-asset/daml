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
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Salt, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.ledger.api.validation.StricterValueValidator as LedgerApiValueValidator
import com.digitalasset.canton.ledger.participant.state.{RepairUpdate, TransactionMeta, Update}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.admin.repair.RepairService.{ContractToAdd, DomainLookup}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.participant.util.DAMLe.ContractWithMetadata
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{LfChoiceName, *}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.time.Instant
import scala.Ordered.orderingToOrdered
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

/** Implements the repair commands.
  * Repair commands work only if the participant has disconnected from the affected domains. Additionally for repair
  * commands, which change the Ledger API events: all domains needs to be disconnected, and the indexer is switched
  * to repair mode.
  * Every individual repair command is executed transactionally, i.e., either all its effects are applied or none.
  * This is achieved by the repair-indexer only changing the Domain Indexes for the affected domains after all previous
  * operations were successful, and the emitted Update events are all persisted. In case of an error during repair,
  * or crash during repair: on node and domain recovery all the changes will be purged.
  * During a repair operation (as domains are disconnected) no new events are visible on the Ledger API, neither the
  * ones added by the ongoing repair. As the repair operation successfully finished new events (if any) will become
  * visible on the Ledger API - Ledger End and domain indexes change, open tailing streams start emitting the repair
  * events if applicable.
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
    contractStore: Eval[ContractStore],
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    aliasManager: DomainAliasManager,
    parameters: ParticipantNodeParameters,
    threadsAvailableForWriting: PositiveInt,
    val domainLookup: DomainLookup,
    @VisibleForTesting
    private[canton] val executionQueue: SimpleExecutionQueue,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private type MissingContract = (SerializableContract, RequestCounter)
  private type MissingAssignment =
    (LfContractId, Source[DomainId], ReassignmentCounter, TimeOfChange)
  private type MissingAdd = (LfContractId, ReassignmentCounter, TimeOfChange)
  private type MissingPurge = (LfContractId, TimeOfChange)

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private def aliasToDomainId(alias: DomainAlias): EitherT[Future, String, DomainId] =
    EitherT.fromEither[Future](
      aliasManager.domainIdForAlias(alias).toRight(s"Could not find $alias")
    )

  private def domainNotConnected(domainId: DomainId): EitherT[Future, String, Unit] = EitherT.cond(
    !domainLookup.isConnected(domainId),
    (),
    s"Participant is still connected to domain $domainId",
  )

  private def contractToAdd(
      repairContract: RepairContract,
      ignoreAlreadyAdded: Boolean,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId

    def addContract(
        reassigningFrom: Option[Source[DomainId]]
    ): EitherT[FutureUnlessShutdown, String, Option[ContractToAdd]] = Right(
      Option(
        ContractToAdd(
          repairContract.contract,
          repairContract.witnesses.map(_.toLf),
          repairContract.reassignmentCounter,
          reassigningFrom,
        )
      )
    ).toEitherT[FutureUnlessShutdown]

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
      case Some(ActiveContractStore.Purged) => addContract(reassigningFrom = None)
      case Some(ActiveContractStore.ReassignedAway(targetDomain, reassignmentCounter)) =>
        log(
          s"Marking contract ${repairContract.contract.contractId} previously unassigned to $targetDomain as " +
            s"assigned from $targetDomain (even though contract may have been reassigned to yet another domain since)."
        ).discard

        val isReassignmentCounterIncreasing =
          repairContract.reassignmentCounter > reassignmentCounter

        if (isReassignmentCounterIncreasing) {
          addContract(reassigningFrom = Option(Source(targetDomain.unwrap)))
        } else {
          EitherT.leftT(
            log(
              s"The reassignment counter ${repairContract.reassignmentCounter} of the contract " +
                s"${repairContract.contract.contractId} needs to be strictly larger than the reassignment counter " +
                s"$reassignmentCounter at the time of the unassignment."
            )
          )
        }

    }
  }

  /** Prepare contract for add, including re-computing metadata
    * @param repairContract Contract to be added
    * @param acsState If the contract is known, its status
    * @param storedContract If the contract already exist in the ContractStore, the stored copy
    */
  private def readRepairContractCurrentState(
      repairContract: RepairContract,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[SerializableContract],
      ignoreAlreadyAdded: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[ContractToAdd]] =
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
        .mapK(FutureUnlessShutdown.outcomeK)
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
  // If this repair request succeeds, it will advance the clean RequestIndex to this time of change.
  // That's why it is important that there are no inflight validation requests before the repair request.
  private def readDomainData(
      domainId: DomainId,
      domainAlias: DomainAlias,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, RepairRequest.DomainData] =
    for {
      persistentState <- EitherT.fromEither[FutureUnlessShutdown](
        lookUpDomainPersistence(domainId, s"domain $domainAlias")
      )
      domainIndex <- EitherT
        .right(
          ledgerApiIndexer.value.ledgerApiStore.value.cleanDomainIndex(domainId)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      startingPoints <- EitherT
        .right(
          SyncDomainEphemeralStateFactory.startingPoints(
            persistentState.requestJournalStore,
            persistentState.sequencedEventStore,
            domainIndex,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      topologyFactory <- domainLookup
        .topologyFactoryFor(domainId)
        .toRight(s"No topology factory for domain $domainAlias")
        .toEitherT[FutureUnlessShutdown]

      topologySnapshot = topologyFactory.createTopologySnapshot(
        startingPoints.processing.prenextTimestamp,
        packageDependencyResolver,
        preferCaching = true,
      )
      domainParameters <- OptionT(persistentState.parameterStore.lastParameters)
        .toRight(log(s"No static domains parameters found for $domainAlias"))
        .mapK(FutureUnlessShutdown.outcomeK)
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
      runConsecutiveAndAwaitUS(
        "repair.add",
        withRepairIndexer { repairIndexer =>
          (for {
            domainId <- EitherT.fromEither[FutureUnlessShutdown](
              aliasManager.domainIdForAlias(domain).toRight(s"Could not find $domain")
            )
            domain <- readDomainData(domainId, domain)

            contractStates <- EitherT.right[String](
              readContractAcsStates(
                domain.persistentState,
                contracts.map(_.contract.contractId),
              )
            )

            storedContracts <- logOnFailureWithInfoLevel(
              contractStore.value.lookupManyUncached(
                contracts.map(_.contract.contractId)
              ),
              "Unable to lookup contracts in contract store",
            )
              .map { contracts =>
                contracts.view.flatMap(_.map(c => c.contractId -> c.contract)).toMap
              }

            filteredContracts <- contracts.zip(contractStates).parTraverseFilter {
              case (contract, acsState) =>
                readRepairContractCurrentState(
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
                _ =>
                  EitherT.rightT[FutureUnlessShutdown, String](
                    logger.info("No contract needs to be added")
                  ),
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

                    _ <- cleanRepairRequests(repair)

                    // Publish added contracts upstream as created via the ledger api.
                    _ <- EitherT.right[String](
                      writeContractsAddedEvents(
                        repair,
                        hostedWitnesses,
                        contractsToAdd,
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
    runConsecutiveAndAwaitUS(
      "repair.purge",
      withRepairIndexer { repairIndexer =>
        (for {
          domainId <- EitherT.fromEither[FutureUnlessShutdown](
            aliasManager.domainIdForAlias(domain).toRight(s"Could not find $domain")
          )
          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          contractStates <- EitherT.right[String](
            readContractAcsStates(
              repair.domain.persistentState,
              contractIds,
            )
          )

          storedContracts <-
            logOnFailureWithInfoLevel(
              contractStore.value.lookupManyUncached(contractIds),
              "Unable to lookup contracts in contract store",
            )
              .map { contracts =>
                contracts.view.flatMap(_.map(c => c.contractId -> c.contract)).toMap
              }

          operationsE = contractIds
            .zip(contractStates)
            .foldMapM { case (cid, acsStatus) =>
              val storedContract = storedContracts.get(cid)
              computePurgeOperations(repair, ignoreAlreadyPurged)(cid, acsStatus, storedContract)
                .map { case (missingPurge, missingAssignment) =>
                  (storedContract.toList, missingPurge, missingAssignment)
                }
            }
          operations <- EitherT.fromEither[FutureUnlessShutdown](operationsE)

          (contractsToPublishUpstream, missingPurges, missingAssignments) = operations

          // Update the stores
          _ <- repair.domain.persistentState.activeContractStore
            .purgeContracts(missingPurges)
            .toEitherTWithNonaborts
            .leftMap(e =>
              log(s"Failed to purge contracts $missingAssignments in ActiveContractStore: $e")
            )
            .mapK(FutureUnlessShutdown.outcomeK)

          _ <- repair.domain.persistentState.activeContractStore
            .assignContracts(missingAssignments)
            .toEitherTWithNonaborts
            .leftMap(e =>
              log(
                s"Failed to assign contracts $missingAssignments in ActiveContractStore: $e"
              )
            )

          _ <- cleanRepairRequests(repair)

          // Publish purged contracts upstream as archived via the ledger api.
          _ <- EitherTUtil.rightUS[String, Unit](
            writeContractsPurgedEvent(contractsToPublishUpstream, Nil, repair, repairIndexer)
          )
        } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("purgeContracts"))
      },
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
      contractIds: NonEmpty[Seq[LfContractId]],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Change assignation request for ${contractIds.length} contracts from $sourceDomain to $targetDomain with skipInactive=$skipInactive"
    )
    runConsecutiveAndAwaitUS(
      "repair.change_assignation",
      for {
        domains <- (
          aliasToDomainId(sourceDomain),
          aliasToDomainId(targetDomain),
        ).tupled
          .mapK(FutureUnlessShutdown.outcomeK)

        (sourceDomainId, targetDomainId) = domains
        _ <- changeAssignation(
          contractIds,
          Source(sourceDomainId),
          Target(targetDomainId),
          skipInactive,
          batchSize,
        )
      } yield (),
    )
  }

  /** Change the assignation of a contract from one domain to another
    *
    * This function here allows us to manually insert a unassignment/assignment into the respective
    * journals in order to move a contract from one domain to another. The procedure will result in
    * a consistent state if and only if all the counter parties run the same command. Failure to do so,
    * will results in participants reporting errors and possibly break.
    *
    * @param skipInactive if true, then the migration will skip contracts in the contractId list that are inactive
    */
  def changeAssignation(
      contractIds: NonEmpty[Seq[LfContractId]],
      sourceDomainId: Source[DomainId],
      targetDomainId: Target[DomainId],
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val numberOfContracts = PositiveInt.tryCreate(contractIds.size)
    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        sourceDomainId.unwrap != targetDomainId.unwrap,
        "Source must differ from target domain!",
      )

      repairSource <- sourceDomainId.traverse(
        initRepairRequestAndVerifyPreconditions(
          _,
          numberOfContracts,
        )
      )

      repairTarget <- targetDomainId.traverse(
        initRepairRequestAndVerifyPreconditions(
          _,
          numberOfContracts,
        )
      )

      _ <- withRepairIndexer { repairIndexer =>
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
          changeAssignationData <- EitherT.fromEither[FutureUnlessShutdown](
            ChangeAssignation.Data.from(contractIds.forgetNE, changeAssignation)
          )

          _ <- cleanRepairRequests(repairTarget.unwrap, repairSource.unwrap)

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          _ <- MonadUtil
            .batchedSequentialTraverse(
              parallelism = threadsAvailableForWriting * PositiveInt.two,
              batchSize,
            )(
              changeAssignationData
            )(changeAssignation.changeAssignation(_, skipInactive).map(_ => Seq[Unit]()))
            .map(_ => ())

        } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("changeAssignation"))
      }
    } yield ()
  }

  def ignoreEvents(
      domain: DomainId,
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    logger.info(s"Ignoring sequenced events from $fromInclusive to $toInclusive (force = $force).")
    runConsecutive(
      "repair.skip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, fromInclusive, force)(
          _.ignoreEvents(fromInclusive, toInclusive).leftMap(_.toString)
        )
      } yield (),
    )
  }

  /** Rollback the Unassignment. The contract is re-assigned to the source domain.
    * The reassignment counter is increased by two. The contract is inserted into the contract store
    * on the target domain if it is not already there. Additionally, we publish the reassignment events.
    */
  def rollbackUnassignment(
      reassignmentId: ReassignmentId,
      target: Target[DomainId],
  )(implicit context: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    withRepairIndexer { repairIndexer =>
      (for {
        sourceRepairRequest <- reassignmentId.sourceDomain.traverse(
          initRepairRequestAndVerifyPreconditions(_)
        )
        targetRepairRequest <- target.traverse(initRepairRequestAndVerifyPreconditions(_))
        reassignmentData <-
          targetRepairRequest.unwrap.domain.persistentState.reassignmentStore
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
          Source(targetRepairRequest.unwrap),
          Target(sourceRepairRequest.unwrap),
          participantId,
          syncCrypto,
          repairIndexer,
          contractStore.value,
          loggerFactory,
        )
        contractIdData <- EitherT.fromEither[FutureUnlessShutdown](
          ChangeAssignation.Data
            .from(
              reassignmentData.contract.contractId,
              changeAssignationBack,
            )
            .incrementRequestCounter
        )
        _ <- changeAssignationBack.changeAssignation(Seq(contractIdData), skipInactive = false)
      } yield ()).mapK(FutureUnlessShutdown.failOnShutdownToAbortExceptionK("rollbackUnassignment"))
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
      _ <- EitherT.right(
        ledgerApiIndexer.value.ensureNoProcessingForDomain(domain)
      )
      domainIndex <- EitherT.right(
        ledgerApiIndexer.value.ledgerApiStore.value.cleanDomainIndex(domain)
      )
      startingPoints <- EitherT.right(
        SyncDomainEphemeralStateFactory.startingPoints(
          persistentState.requestJournalStore,
          persistentState.sequencedEventStore,
          domainIndex,
        )
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
      fromInclusive: SequencerCounter,
      toInclusive: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    logger.info(
      s"Unignoring sequenced events from $fromInclusive to $toInclusive (force = $force)."
    )
    runConsecutive(
      "repair.unskip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, fromInclusive, force)(
          sequencedEventStore =>
            sequencedEventStore.unignoreEvents(fromInclusive, toInclusive).leftMap(_.toString)
        )
      } yield (),
    )
  }

  private def useComputedContractAndMetadata(
      inputContract: SerializableContract,
      computed: ContractWithMetadata,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SerializableContract] =
    EitherT.fromEither[FutureUnlessShutdown](
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
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
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
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val topologySnapshot = repair.domain.topologySnapshot
    val contract = contractToAdd.contract
    val contractId = contractToAdd.cid
    for {
      _warnOnEmptyMaintainers <- EitherT.cond[FutureUnlessShutdown](
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

      _witnessesKnownLocally <- EitherT.fromEither[FutureUnlessShutdown](_witnessesKnownLocallyE)

      _ <-
        if (ignoreStakeholderCheck) EitherT.rightT[FutureUnlessShutdown, String](())
        else
          for {
            // At least one stakeholder is hosted locally if no witnesses are defined
            _localStakeholderOrWitnesses <- EitherT
              .right(
                hostsParties(topologySnapshot, contract.metadata.stakeholders, participantId)
              )
              .map { localStakeholders =>
                Either.cond(
                  contractToAdd.witnesses.nonEmpty || localStakeholders.nonEmpty,
                  (),
                  log(
                    s"Contract ${contract.contractId} has no local stakeholders ${contract.metadata.stakeholders} and no witnesses defined"
                  ),
                )
              }

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
      _ <- topologySnapshot
        .allHaveActiveParticipants(contractToAdd.witnesses)
        .leftMap { missingWitnesses =>
          log(
            s"Domain ${repair.domain.alias} missing witnesses $missingWitnesses of contract ${contract.contractId}"
          )
        }
    } yield ()
  }

  private def logOnFailureWithInfoLevel[T](f: Future[T], errorMessage: => String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, T] =
    EitherTUtil.rightUS(
      FutureUtil.logOnFailure(f, errorMessage, level = Level.INFO)
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
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      // We compute first which changes we need to persist
      missingContracts <- contractsToAdd
        .parTraverseFilter[EitherT[FutureUnlessShutdown, String, *], MissingContract] {
          case (contractToAdd, toc) =>
            storedContracts.get(contractToAdd.cid) match {
              case None =>
                EitherT.pure[FutureUnlessShutdown, String](
                  Some((contractToAdd.contract, toc.rc))
                )

              case Some(storedContract) =>
                EitherTUtil
                  .condUnitET[FutureUnlessShutdown](
                    storedContract == contractToAdd.contract,
                    s"Contract ${contractToAdd.cid} already exists in the contract store, but differs from contract to be created. Contract to be created $contractToAdd versus existing contract $storedContract.",
                  )
                  .map(_ => Option.empty[MissingContract])
            }
        }

      (missingAssignments, missingAdds) = contractsToAdd.foldLeft(
        (Seq.empty[MissingAssignment], Seq.empty[MissingAdd])
      ) { case ((missingAssignments, missingAdds), (contract, toc)) =>
        contract.reassigningFrom match {
          case Some(sourceDomainId) =>
            val newAssignment = (contract.cid, sourceDomainId, contract.reassignmentCounter, toc)
            (newAssignment +: missingAssignments, missingAdds)

          case None =>
            val newAdd = (contract.cid, contract.reassignmentCounter, toc)
            (missingAssignments, newAdd +: missingAdds)
        }
      }

      // Now, we update the stores
      _ <- logOnFailureWithInfoLevel(
        contractStore.value.storeCreatedContracts(missingContracts),
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
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- repair.domain.persistentState.activeContractStore
        .assignContracts(missingAssignments)
        .toEitherTWithNonaborts
        .leftMap(e =>
          log(
            s"Failed to assign ${missingAssignments.map { case (cid, _, _, _) => cid }} in ActiveContractStore: $e"
          )
        )
    } yield ()

  /** For the given contract, returns the operations (purge, assignment) to perform
    * @param acsStatus Status of the contract
    * @param storedContractO Instance of the contract
    */
  private def computePurgeOperations(repair: RepairRequest, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId,
      acsStatus: Option[ActiveContractStore.Status],
      storedContractO: Option[SerializableContract],
  )(implicit
      traceContext: TraceContext
  ): Either[String, (Seq[MissingPurge], Seq[MissingAssignment])] = {
    def ignoreOrError(reason: String): Either[String, (Seq[MissingPurge], Seq[MissingAssignment])] =
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
          (Seq[MissingPurge]((cid, toc)), Seq.empty[MissingAssignment])
        }
      case Some(ActiveContractStore.Archived) => ignoreOrError("archived contract")
      case Some(ActiveContractStore.Purged) => ignoreOrError("purged contract")
      case Some(ActiveContractStore.ReassignedAway(targetDomain, reassignmentCounter)) =>
        log(
          s"Purging contract $cid previously marked as reassigned to $targetDomain. " +
            s"Marking contract as assigned from $targetDomain (even though contract may have since been reassigned to yet another domain) and subsequently as archived."
        ).discard

        reassignmentCounter.increment.map { newReassignmentCounter =>
          (
            Seq[MissingPurge]((cid, toc)),
            Seq[MissingAssignment](
              (
                cid,
                Source(targetDomain.unwrap),
                newReassignmentCounter,
                toc,
              )
            ),
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
      hostedWitnesses: Seq[LfPartyId],
      repair: RepairRequest,
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contracts.map(toArchive)).toMap
    val update = Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = repair.timestamp.toLf,
        workflowId = None,
        submissionTime = repair.timestamp.toLf,
        submissionSeed = Update.noOpSeed,
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
      updateId = repair.transactionId.tryAsLedgerTransactionId,
      hostedWitnesses = hostedWitnesses.toList,
      contractMetadata = Map.empty,
      domainId = repair.domain.id,
      requestCounter = repair.tryExactlyOneRequestCounter,
      recordTime = repair.timestamp,
    )
    // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
    repairIndexer.offer(update).map(_ => ())
  }

  private def prepareAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      requestCounter: RequestCounter,
      ledgerCreateTime: LedgerCreateTime,
      contractsAdded: Seq[ContractToAdd],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): RepairUpdate = {
    val contractMetadata = contractsAdded.view
      .map(c => c.contract.contractId -> c.driverMetadata(repair.domain.parameters.protocolVersion))
      .toMap
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(contractsAdded.map(_.contract.toLf)).toMap
    Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = ledgerCreateTime.toLf,
        workflowId = workflowIdProvider(),
        submissionTime = repair.timestamp.toLf,
        submissionSeed = Update.noOpSeed,
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
      updateId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId,
      hostedWitnesses = contractsAdded.flatMap(_.witnesses.intersect(hostedParties)).toList,
      contractMetadata = contractMetadata,
      domainId = repair.domain.id,
      requestCounter = requestCounter,
      recordTime = repair.timestamp,
    )
  }

  private def writeContractsAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      contractsAdded: Seq[(TimeOfChange, (LedgerCreateTime, Seq[ContractToAdd]))],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(MonadUtil.sequentialTraverse_(contractsAdded) {
      case (timeOfChange, (timestamp, contractsToAdd)) =>
        // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
        repairIndexer
          .offer(
            prepareAddedEvents(
              repair,
              hostedParties,
              timeOfChange.rc,
              timestamp,
              contractsToAdd,
              () => workflowIds.next(),
            )
          )
          .map(_ => ())
    })

  private def packageKnown(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      packageDescription <- EitherTUtil.rightUS(
        packageDependencyResolver.getPackageDescription(lfPackageId)
      )
      _packageVetted <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          packageDescription.nonEmpty,
          log(s"Failed to locate package $lfPackageId"),
        )
    } yield ()

  /** Allows to wait until clean head has progressed up to a certain timestamp */
  def awaitCleanHeadForTimestamp(
      domainId: DomainId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    def check(persistentState: SyncDomainPersistentState): Future[Either[String, Unit]] =
      ledgerApiIndexer.value.ledgerApiStore.value
        .cleanDomainIndex(domainId)
        .flatMap(
          SyncDomainEphemeralStateFactory.startingPoints(
            persistentState.requestJournalStore,
            persistentState.sequencedEventStore,
            _,
          )
        )
        .map { startingPoints =>
          if (startingPoints.processing.prenextTimestamp >= timestamp) {
            logger.debug(
              s"Clean head reached ${startingPoints.processing.prenextTimestamp}, clearing $timestamp"
            )
            Either.unit
          } else {
            logger.debug(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet $timestamp"
            )
            Left(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet $timestamp"
            )
          }
        }
    EitherT
      .fromEither[FutureUnlessShutdown](
        lookUpDomainPersistence(domainId, s"domain $domainId")
      )
      .flatMap { persistentState =>
        EitherT(
          retry
            .Pause(
              logger,
              this,
              retry.Forever,
              50.milliseconds,
              s"awaiting clean-head for=$domainId at ts=$timestamp",
            )
            .unlessShutdown(
              FutureUnlessShutdown.outcomeF(check(persistentState)),
              AllExceptionRetryPolicy,
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

  /** Repair commands are inserted where processing starts again upon reconnection.
    *
    * @param domainId The ID of the domain for which the request is valid
    * @param requestCountersToAllocate The number of request counters to allocate in order to fulfill the request
    */
  private def initRepairRequestAndVerifyPreconditions(
      domainId: DomainId,
      requestCountersToAllocate: PositiveInt = PositiveInt.one,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, RepairRequest] =
    for {
      domainAlias <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .aliasForDomainId(domainId)
          .toRight(s"domain alias for $domainId not found")
      )
      domainData <- readDomainData(domainId, domainAlias)
      repairRequest <- initRepairRequestAndVerifyPreconditions(
        domainData,
        requestCountersToAllocate,
      )
    } yield repairRequest

  private def initRepairRequestAndVerifyPreconditions(
      domain: RepairRequest.DomainData,
      requestCountersToAllocate: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, RepairRequest] = {
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
      _ <- EitherT
        .right(
          SyncDomainEphemeralStateFactory
            .cleanupPersistentState(domain.persistentState, domain.startingPoints)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      incrementalAcsSnapshotWatermark <- EitherT.right(
        domain.persistentState.acsCommitmentStore.runningCommitments.watermark
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        rtRepair > incrementalAcsSnapshotWatermark,
        (),
        log(
          s"""Cannot apply a repair command as the incremental acs snapshot is already at $incrementalAcsSnapshotWatermark
             |and the repair command would be assigned a record time of $rtRepair.
             |Reconnect to the domain to reprocess inflight validation requests and retry repair afterwards.""".stripMargin
        ),
      )
      requestCounters <- EitherT.fromEither[FutureUnlessShutdown](
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
      _ <- EitherT
        .right[String](
          repair.requestData.parTraverse_(domain.persistentState.requestJournalStore.insert)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

    } yield repair
  }

  private def markClean(
      repair: RepairRequest
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
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
      .mapK(FutureUnlessShutdown.outcomeK)

  private def cleanRepairRequests(
      repairs: RepairRequest*
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- repairs.parTraverse_(markClean)
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
  ): FutureUnlessShutdown[Seq[Option[ActiveContractStore.Status]]] =
    persistentState.activeContractStore
      .fetchStates(cids)
      .map { states =>
        cids.map(cid => states.get(cid).map(_.status))
      }

  // Looks up domain persistence erroring if domain is based on in-memory persistence for which repair is not supported.
  private def lookUpDomainPersistence(domainId: DomainId, domainDescription: String)(implicit
      traceContext: TraceContext
  ): Either[String, SyncDomainPersistentState] =
    for {
      dp <- domainLookup
        .persistentStateFor(domainId)
        .toRight(log(s"Could not find $domainDescription"))
      _ <- Either.cond(
        !dp.isMemory,
        (),
        log(
          s"$domainDescription is in memory which is not supported by repair. Use db persistence."
        ),
      )
    } yield dp

  private def runConsecutiveAndAwaitUS[B](
      description: String,
      code: => EitherT[FutureUnlessShutdown, String, B],
  )(implicit
      traceContext: TraceContext
  ): Either[String, B] = {
    logger.info(s"Queuing $description")

    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.awaitUS(description) {
      logger.info(s"Queuing $description")
      executionQueue.executeEUS(code, description).value
    }
  }.onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  private def runConsecutive[B](
      description: String,
      code: => EitherT[Future, String, B],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, B] = {
    logger.info(s"Queuing $description")
    EitherT(
      executionQueue
        .executeE(code, description)
        .value
        .onShutdown(Left(s"$description aborted due to shutdown"))
    )
  }

  private def log(message: String)(implicit traceContext: TraceContext): String = {
    // consider errors user errors and log them on the server side as info:
    logger.info(message)
    message
  }

  override protected def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)

  private def withRepairIndexer(code: FutureQueue[RepairUpdate] => EitherT[Future, String, Unit])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    if (domainLookup.isConnectedToAnyDomain) {
      EitherT.leftT[FutureUnlessShutdown, Unit](
        "There are still domains connected. Please disconnect all domains."
      )
    } else {
      ledgerApiIndexer.value
        .withRepairIndexer(code)
        .mapK(FutureUnlessShutdown.outcomeK)
    }
}

object RepairService {

  object ContractConverter extends HasLoggerName {

    def contractDataToInstance(
        templateId: Identifier,
        packageName: LfPackageName,
        packageVersion: Option[LfPackageVersion],
        createArguments: Record,
        signatories: Set[String],
        observers: Set[String],
        lfContractId: LfContractId,
        ledgerTime: Instant,
        contractSalt: Option[Salt],
    )(implicit namedLoggingContext: NamedLoggingContext): Either[String, SerializableContract] =
      for {
        template <- LedgerApiValueValidator.validateIdentifier(templateId).leftMap(_.getMessage)

        argsValue <- LedgerApiValueValidator
          .validateRecord(createArguments)
          .leftMap(e => s"Failed to validate arguments: $e")

        argsVersionedValue = LfVersioned(
          protocol.DummyTransactionVersion, // Version is ignored by daml engine upon RepairService.addContract
          argsValue,
        )

        lfContractInst = LfContractInst(
          packageName = packageName,
          template = template,
          packageVersion = packageVersion,
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

    def contractInstanceToData(
        contract: SerializableContract
    ): Either[
      String,
      (
          Identifier,
          LfPackageName,
          Option[LfPackageVersion],
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
            s"Failed to convert contract instance to data due to issue with create-arguments: $e",
          record => {
            val signatories = contract.metadata.signatories.map(_.toString)
            val stakeholders = contract.metadata.stakeholders.map(_.toString)
            (
              LfEngineToApi.toApiIdentifier(contractInstance.unversioned.template),
              contractInstance.unversioned.packageName,
              contractInstance.unversioned.packageVersion,
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
      reassignmentCounter: ReassignmentCounter,
      reassigningFrom: Option[Source[DomainId]],
  ) {
    def cid: LfContractId = contract.contractId

    def driverMetadata(protocolVersion: ProtocolVersion): Bytes =
      contract.contractSalt
        .map(DriverContractMetadata(_).toLfBytes(protocolVersion))
        .getOrElse(Bytes.Empty)
  }

  trait DomainLookup {
    def isConnected(domainId: DomainId): Boolean

    def isConnectedToAnyDomain: Boolean

    def persistentStateFor(domainId: DomainId): Option[SyncDomainPersistentState]

    def topologyFactoryFor(domainId: DomainId)(implicit
        traceContext: TraceContext
    ): Option[TopologyComponentFactory]
  }
}
