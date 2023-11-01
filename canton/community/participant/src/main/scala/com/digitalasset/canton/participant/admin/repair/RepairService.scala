// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Salt, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.ledger.api.validation.{
  FieldValidator as LedgerApiFieldValidations,
  StricterValueValidator as LedgerApiValueValidator,
}
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
import com.digitalasset.canton.platform.participant.util.LfEngineToApi
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
    indexedStringStore: IndexedStringStore,
    isConnected: DomainId => Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  // Ensure only one repair runs at a time. This ensures concurrent activity among repair operations does
  // not corrupt state.
  private val executionQueue = new SimpleExecutionQueue(
    "repair-service-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

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
      domain: RepairRequest.DomainData,
      repairContract: RepairContract,
      ignoreAlreadyAdded: Boolean,
      acsState: Option[ActiveContractStore.Status],
      keyState: Option[RepairService.KeyState],
  )(implicit traceContext: TraceContext): EitherT[Future, String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId
    acsState match {
      case None =>
        keyState
          .traverse(_.checkAssignable(domain))
          .map { keyToAssign =>
            Option(
              ContractToAdd(
                repairContract.contract,
                repairContract.witnesses.map(_.toLf),
                repairContract.transferCounter,
                None,
                keyToAssign,
              )
            )
          }
          .toEitherT[Future]
      case Some(ActiveContractStore.Active(_)) =>
        if (ignoreAlreadyAdded) {
          logger.debug(s"Skipping contract $contractId because it is already active")
          for {
            contractAlreadyThere <-
              domain.persistentState.contractStore
                .lookupContractE(contractId)
                .leftMap(e =>
                  log(s"Failed to look up contract $contractId in domain ${domain.alias}: $e")
                )
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
      case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
        log(
          s"Marking contract ${repairContract.contract.contractId} previously transferred-out to $targetDomain as " +
            s"transferred-in from $targetDomain (even though contract may have been transferred to yet another domain since)."
        ).discard

        val isTransferCounterIncreasing = (transferCounter, repairContract.transferCounter)
          .mapN { case (tc, repairTc) => repairTc > tc }
          .getOrElse(true)

        if (isTransferCounterIncreasing) {
          keyState
            .traverse(_.checkAssignable(domain))
            .map { keyToAssign =>
              Option(
                ContractToAdd(
                  repairContract.contract,
                  repairContract.witnesses.map(_.toLf),
                  repairContract.transferCounter,
                  Option(SourceDomainId(targetDomain.unwrap)),
                  keyToAssign,
                )
              )
            }
            .toEitherT[Future]
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

  private def readRepairContractCurrentState(
      domain: RepairRequest.DomainData,
      repairContract: RepairContract,
      ignoreAlreadyAdded: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[ContractToAdd]] =
    for {
      acsState <- readContractAcsState(domain.persistentState, repairContract.contract.contractId)
      keyState <- EitherT.liftF(readContractKey(domain, repairContract.contract))
      // Able to recompute contract signatories and stakeholders (and sanity check
      // repairContract metadata otherwise ignored matches real metadata)
      contractWithMetadata <- damle
        .contractWithMetadata(
          repairContract.contract.rawContractInstance.contractInstance,
          repairContract.contract.metadata.signatories,
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
        domain,
        repairContract.copy(contract = computedContract),
        ignoreAlreadyAdded,
        acsState,
        keyState,
      )
    } yield contractToAdd

  // The repair request gets inserted at the reprocessing starting point.
  // We use the prenextTimestamp such that a regular request is always the first request for a given timestamp.
  // This is needed for causality tracking, which cannot use a tie breaker on timestamps.
  //
  // If this repair request succeeds, it will advance the clean request prehead to this time of change.
  // That's why it is important that there are no dirty requests before the repair request.
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
        packageId => packageDependencyResolver.packageDependencies(List(packageId)),
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
    */
  def addContracts(
      domain: DomainAlias,
      contracts: Seq[RepairContract],
      ignoreAlreadyAdded: Boolean,
      ignoreStakeholderCheck: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Adding ${contracts.length} contracts to domain ${domain} with ignoreAlreadyAdded=${ignoreAlreadyAdded} and ignoreStakeholderCheck=${ignoreStakeholderCheck}"
    )
    if (contracts.isEmpty) {
      Either.right(logger.info("No contracts to add specified"))
    } else {
      lockAndAwaitEitherT(
        "repair.add", {
          for {
            // Ensure domain is configured but not connected to avoid race conditions.
            domainId <- aliasToUnconnectedDomainId(domain)

            domain <- readDomainData(domainId)

            filteredContracts <- contracts.parTraverseFilter(
              readRepairContractCurrentState(domain, _, ignoreAlreadyAdded)
            )

            contractsByCreation = filteredContracts
              .groupBy(_.contract.ledgerCreateTime)
              .toList
              .sortBy { case (ledgerCreateTime, _) => ledgerCreateTime }

            _ <- PositiveInt
              .create(contractsByCreation.size)
              .fold(
                _ => EitherT.rightT[Future, String](logger.info("No contract needs to be added")),
                requestCountersToAllocate => {
                  for {
                    repair <- initRepairRequestAndVerifyPreconditions(
                      domain,
                      requestCountersToAllocate,
                    )

                    contractsToAdd = repair.timesOfChange.zip(contractsByCreation)

                    // All referenced templates known and vetted
                    _packagesVetted <- filteredContracts
                      .map(
                        _.contract.rawContractInstance.contractInstance.unversioned.template.packageId
                      )
                      .distinct
                      .parTraverse_(packageVetted)

                    _uniqueKeysWithHostedMaintainerInContractsToAdd <- EitherTUtil.ifThenET(
                      repair.domain.parameters.uniqueContractKeys
                    ) {
                      val keysWithContractIdsF = filteredContracts
                        .parTraverseFilter { repairContractWithCurrentState =>
                          val contract = repairContractWithCurrentState.contract
                          // Only check for duplicates where the participant hosts a maintainer
                          getKeyIfOneMaintainerIsLocal(
                            repair.domain.topologySnapshot,
                            contract.metadata.maybeKeyWithMaintainers,
                            participantId,
                          ).map { lfKeyO =>
                            lfKeyO.flatMap(_ => contract.metadata.maybeKeyWithMaintainers).map {
                              keyWithMaintainers =>
                                keyWithMaintainers.globalKey -> contract.contractId
                            }
                          }
                        }
                        .map(x => x.groupBy { case (globalKey, _) => globalKey })
                      EitherT(keysWithContractIdsF.map { keysWithContractIds =>
                        val duplicates = keysWithContractIds.mapFilter { keyCoids =>
                          if (keyCoids.lengthCompare(1) > 0) Some(keyCoids.map(_._2)) else None
                        }
                        Either.cond(
                          duplicates.isEmpty,
                          (),
                          log(show"Cannot add multiple contracts for the same key(s): $duplicates"),
                        )
                      })
                    }

                    hostedParties <- EitherT.right(
                      filteredContracts
                        .flatMap(_.witnesses)
                        .distinct
                        .parFilterA(hostsParty(repair.domain.topologySnapshot, participantId))
                        .map(_.toSet)
                    )

                    _ = logger.debug(s"Publishing ${filteredContracts.size} added contracts")

                    contractsWithTimeOfChange = contractsToAdd.flatMap { case (toc, (_, cs)) =>
                      cs.map(_ -> toc)
                    }

                    // Note the following purposely fails if any contract fails which results in not all contracts being processed.
                    _ <- MonadUtil.sequentialTraverse(contractsWithTimeOfChange) {
                      case (contract, timeOfChange) =>
                        addContract(repair, ignoreStakeholderCheck)(contract, timeOfChange)
                    }

                    // Publish added contracts upstream as created via the ledger api.
                    _ <- EitherT.right(
                      writeContractsAddedEvents(repair, hostedParties, contractsToAdd)
                    )

                    // If all has gone well, bump the clean head, effectively committing the changes to the domain.
                    _ <- commitRepairs(repair)

                  } yield ()
                },
              )
          } yield ()
        },
      )
    }
  }

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
    lockAndAwaitEitherT(
      "repair.purge", {
        for {
          domainId <- aliasToUnconnectedDomainId(domain)
          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          contractsToPublishUpstream <- MonadUtil
            .sequentialTraverse(contractIds)(purgeContract(repair, ignoreAlreadyPurged))
            .map(_.collect { case Some(x) => x })

          // Publish purged contracts upstream as archived via the ledger api.
          _ <- EitherT.right(writeContractsPurgedEvent(contractsToPublishUpstream, Nil, repair))

          // If all has gone well, bump the clean head, effectively committing the changes to the domain.
          _ <- commitRepairs(repair)

        } yield ()
      },
    )
  }

  /** Participant repair utility for manually moving contracts from a source domain to a target domain in an offline
    * fashion.
    *
    * @param contractIds    ids of contracts to move that reside in the sourceDomain
    * @param sourceDomain alias of source domain from which to move contracts
    * @param targetDomain alias of target domain to which to move contracts
    * @param skipInactive   whether to only move contracts that are active in the source domain
    * @param batchSize      how big the batches should be used during the migration
    */
  def changeDomainAwait(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Change domain request for ${contractIds.length} contracts from ${sourceDomain} to ${targetDomain} with skipInactive=${skipInactive}"
    )
    lockAndAwaitEitherT(
      "repair.change_domain", {
        for {
          sourceDomainId <- aliasToUnconnectedDomainId(sourceDomain)
          targetDomainId <- aliasToUnconnectedDomainId(targetDomain)
          _ <- changeDomain(
            contractIds,
            sourceDomainId,
            targetDomainId,
            skipInactive,
            batchSize,
          )
        } yield ()
      },
    )
  }

  /** Change the association of a contract from one domain to another
    *
    * This function here allows us to manually insert a transfer out / in into the respective
    * journals in order to move a contract from one domain to another. The procedure will result in
    * a consistent state if and only if all the counter parties run the same command. Failure to do so,
    * will results in participants reporting errors and possibly break.
    *
    * @param skipInactive if true, then the migration will skip contracts in the contractId list that are inactive
    */
  def changeDomain(
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

          migration =
            contractIds
              .zip(repairSource.timesOfChange)
              .zip(repairTarget.timesOfChange)
              .map { case (((contractId, sourceToc), targetToc)) =>
                MigrateContracts.Data(contractId, sourceToc, targetToc)
              }

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          _ <- migrateContractsBatched(
            migration,
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
    lockAndAwaitEitherT(
      "repair.skip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(
          _.ignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
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
    lockAndAwaitEitherT(
      "repair.unskip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(sequencedEventStore =>
          sequencedEventStore.unignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
    )
  }

  private def useComputedContractAndMetadata(
      inputContract: SerializableContract,
      computed: ContractWithMetadata,
  )(implicit traceContext: TraceContext): EitherT[Future, String, SerializableContract] =
    EitherT.fromEither[Future](
      for {
        rawContractInstance <- SerializableRawContractInstance
          .create(computed.instance, computed.agreementText)
          .leftMap(err =>
            log(s"Failed to serialize contract ${inputContract.contractId}: ${err.errorMessage}")
          )
      } yield inputContract.copy(
        metadata = computed.metadataWithGlobalKey,
        rawContractInstance = rawContractInstance,
      )
    )

  private def addContract(
      repair: RepairRequest,
      ignoreStakeholderCheck: Boolean,
  )(
      contractToAdd: ContractToAdd,
      timeOfChange: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    val topologySnapshot = repair.domain.topologySnapshot
    val contract = contractToAdd.contract
    val contractId = contract.contractId
    for {
      _warnOnEmptyMaintainers <- EitherT.cond[Future](
        !contract.metadata.maybeKeyWithMaintainers.exists(_.maintainers.isEmpty),
        (),
        log(s"Contract $contractId has key without maintainers."),
      )

      // Witnesses all known locally.
      _witnessesKnownLocally <- contractToAdd.witnesses.toList.parTraverse_ { p =>
        EitherT(hostsParty(topologySnapshot, participantId)(p).map { hosted =>
          EitherUtil.condUnitE(
            hosted,
            log(s"Witness ${p} not active on domain ${repair.domain.alias} and local participant"),
          )
        })
      }

      _ <-
        if (ignoreStakeholderCheck) EitherT.rightT[Future, String](())
        else
          for {
            // At least one stakeholder is hosted locally if no witnesses are defined
            _localStakeholderOrWitnesses <- EitherT(
              contract.metadata.stakeholders.toList
                .findM(hostsParty(topologySnapshot, participantId))
                .map(_.isDefined)
                .map { oneStakeholderIsLocal =>
                  EitherUtil.condUnitE(
                    contractToAdd.witnesses.nonEmpty || oneStakeholderIsLocal,
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
            s"Domain ${repair.domain.alias} missing witnesses ${missingWitnesses} of contract ${contract.contractId}"
          )
      }

      // Persist contract if it does not already exist. Make sure to use computed rather than input metadata.
      _createdContract <- persistContract(
        repair,
        timeOfChange.rc,
        contract,
      )

      _persistedInCkj <- contractToAdd.keyToAssign.traverse_ { key =>
        repair.domain.persistentState.contractKeyJournal
          .addKeyStateUpdates(Map(key -> (ContractKeyJournal.Assigned, timeOfChange)))
          .leftMap(err => log(s"Error while persisting key state updates: $err"))
      }

      _persistedInAcs <- contractToAdd.transferringFrom
        .map(_ -> contractToAdd.transferCounter)
        .fold(
          persistCreation(
            repair,
            contractId,
            contractToAdd.transferCounter,
            timeOfChange,
          )
        ) { case (sourceDomainId, transferCounter) =>
          persistTransferIn(
            repair,
            sourceDomainId,
            contractId,
            transferCounter,
            timeOfChange,
          )
        }
    } yield ()
  }

  private def purgeContract(repair: RepairRequest, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Option[SerializableContract]] = {
    val timeOfChange = repair.tryExactlyOneTimeOfChange
    for {
      acsStatus <- readContractAcsState(repair.domain.persistentState, cid)

      contractO <- EitherTUtil.fromFuture(
        repair.domain.persistentState.contractStore.lookupContract(cid).value,
        t => log(s"Failed to look up contract ${cid} in domain ${repair.domain.alias}", t),
      )
      // Not checking that the participant hosts a stakeholder as we might be cleaning up contracts
      // on behalf of stakeholders no longer around.
      contractToArchiveInEvent <- acsStatus match {
        case None =>
          EitherT.cond[Future](
            ignoreAlreadyPurged,
            None,
            log(
              s"Contract $cid does not exist in domain ${repair.domain.alias} and cannot be purged. Set ignoreAlreadyPurged = true to skip non-existing contracts."
            ),
          )
        case Some(ActiveContractStore.Active(_)) =>
          for {
            contract <- EitherT
              .fromOption[Future](
                contractO,
                log(show"Active contract $cid not found in contract store"),
              )

            keyOfHostedMaintainerO <- EitherT.liftF(
              if (repair.domain.parameters.uniqueContractKeys)
                getKeyIfOneMaintainerIsLocal(
                  repair.domain.topologySnapshot,
                  contract.metadata.maybeKeyWithMaintainers,
                  participantId,
                )
              else Future.successful(None)
            )

            _ <- keyOfHostedMaintainerO.traverse_ { key =>
              repair.domain.persistentState.contractKeyJournal
                .addKeyStateUpdates(Map(key -> (ContractKeyJournal.Unassigned, timeOfChange)))
                .leftMap(err => log(s"Error while persisting key state updates: $err"))
            }
            _ <- persistArchival(repair, timeOfChange)(cid)
          } yield {
            logger.info(
              s"purged contract $cid at repair request ${repair.tryExactlyOneRequestCounter} at ${repair.timestamp}"
            )
            contractO
          }
        case Some(ActiveContractStore.Archived) =>
          EitherT.cond[Future](
            ignoreAlreadyPurged,
            None,
            log(
              s"Contract $cid is already archived in domain ${repair.domain.alias} and cannot be purged. Set ignoreAlreadyPurged = true to skip archived contracts."
            ),
          )
        case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
          log(
            s"Purging contract $cid previously marked as transferred away to $targetDomain. " +
              s"Marking contract as transferred-in from $targetDomain (even though contract may have since been transferred to yet another domain) and subsequently as archived."
          ).discard
          for {
            newTransferCounter <- EitherT.fromEither[Future](transferCounter.traverse(_.increment))
            sourceDomain = SourceDomainId(targetDomain.unwrap)
            _ <- persistTransferIn(repair, sourceDomain, cid, newTransferCounter, timeOfChange)
            _ <- persistArchival(repair, timeOfChange)(cid)
          } yield contractO
      }
    } yield contractToArchiveInEvent
  }

  private def migrateContractsBatched(
      contractIds: Seq[MigrateContracts.Data[LfContractId]],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    MonadUtil
      .batchedSequentialTraverse(parameters.maxDbConnections * PositiveInt.two, batchSize)(
        contractIds
      )(
        MigrateContracts(
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

  private def persistContract(
      repair: RepairRequest,
      rc: RequestCounter,
      contract: SerializableContract,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {

    def fromFuture[T](f: Future[T], errorMessage: => String): EitherT[Future, String, T] =
      EitherTUtil.fromFuture(
        f,
        t => log(s"$errorMessage: ${ErrorUtil.messageWithStacktrace(t)}"),
      )

    for {
      // Write contract if it does not already exist.
      alreadyExistingContract <- fromFuture(
        repair.domain.persistentState.contractStore.lookupContract(contract.contractId).value,
        s"Failed to check if contract ${contract.contractId} already exists in ContractStore",
      )

      _createdContract <- alreadyExistingContract.fold(
        fromFuture(
          repair.domain.persistentState.contractStore
            .storeCreatedContract(rc, repair.transactionId, contract),
          s"Failed to store contract ${contract.contractId} in ContractStore",
        )
      )(storedContract =>
        EitherTUtil.condUnitET[Future](
          storedContract == contract,
          s"Contract ${contract.contractId} already exists in the contract store, but differs from contract to be created. Contract to be created ${contract} versus existing contract ${storedContract}.",
        )
      )

    } yield ()
  }

  // TODO(#4001) - performance of point-db lookups will be slow, add batching (also to purgeContract and moveContract)
  private def persistCreation(
      repair: RepairRequest,
      cid: LfContractId,
      transferCounter: TransferCounterO,
      timeOfChange: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    repair.domain.persistentState.activeContractStore
      .markContractActive(
        cid -> transferCounter,
        timeOfChange,
      )
      .toEitherTWithNonaborts
      .leftMap(e => log(s"Failed to create contract $cid in ActiveContractStore: $e"))
  }

  private def persistTransferIn(
      repair: RepairRequest,
      sourceDomain: SourceDomainId,
      cid: LfContractId,
      transferCounter: TransferCounterO,
      timeOfChange: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    repair.domain.persistentState.activeContractStore
      .transferInContract(cid, timeOfChange, sourceDomain, transferCounter)
      .toEitherTWithNonaborts
      .leftMap(e => log(s"Failed to transfer in contract ${cid} in ActiveContractStore: ${e}"))

  /*
  We do not store transfer counters for archivals in the repair service,
  given that we do not store them as part of normal transaction processing either.
  The latter is due to the fact that we do not know the correct transfer counters
  at the time we persist the deactivation in the ACS.
   */
  private def persistArchival(
      repair: RepairRequest,
      timeOfChange: TimeOfChange,
  )(cid: LfContractId)(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    repair.domain.persistentState.activeContractStore
      .archiveContract(cid, timeOfChange)
      .toEitherT // not turning warnings to errors on behalf of archived contracts, in contract to created contracts
      .leftMap(e => log(s"Failed to mark contract $cid as archived: $e"))

  // TODO(i12964) once the Ledger API moves away from fake transactions, remove this
  private def toArchive(c: SerializableContract): LfNodeExercises = LfNodeExercises(
    targetCoid = c.contractId,
    templateId = c.rawContractInstance.contractInstance.unversioned.template,
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
    // Not setting the contract key as the indexer deletes contract keys along with contracts.
    // If the contract keys were needed, we'd have to reinterpret the contract to look up the key.
    keyOpt = None,
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

  private def writeContractsAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      requestCounter: RequestCounter,
      ledgerCreateTime: LedgerCreateTime,
      contractsAdded: Seq[ContractToAdd],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val transactionId = randomTransactionId(syncCrypto).tryAsLedgerTransactionId
    val offset = RequestOffset(repair.timestamp, requestCounter)
    val contractMetadata = contractsAdded.view
      .map(c => c.contract.contractId -> c.driverMetadata(repair.domain.parameters.protocolVersion))
      .toMap
    val event =
      TimestampedEvent(
        LedgerSyncEvent.ContractsAdded(
          transactionId = transactionId,
          contracts = contractsAdded.map(_.contract.toLf),
          domainId = repair.domain.id,
          recordTime = repair.timestamp.toLf,
          ledgerTime = ledgerCreateTime.toLf,
          hostedWitnesses = contractsAdded.flatMap(_.witnesses.intersect(hostedParties)),
          contractMetadata = contractMetadata,
        ),
        offset,
        None,
      )

    repair.domain.persistentState.eventLog.eventByTransactionId(transactionId).value.flatMap {
      _.fold(repair.domain.persistentState.eventLog.insert(event)) {
        case `event` =>
          Future {
            logger.info(
              show"Skipping duplicate publication of event at offset $offset with transaction id $transactionId."
            )
          }
        case mismatchedEvent =>
          ErrorUtil.internalError(
            new IllegalArgumentException(
              show"Unable to publish event at offset $offset, as it has the same transaction id $transactionId as the previous event with offset ${mismatchedEvent.localOffset}."
            )
          )
      }
    }
  }

  private def writeContractsAddedEvents(
      repair: RepairRequest,
      hostedParties: Set[LfPartyId],
      contractsAdded: Seq[(TimeOfChange, (LedgerCreateTime, Seq[ContractToAdd]))],
  )(implicit traceContext: TraceContext): Future[Unit] =
    MonadUtil.sequentialTraverse_(contractsAdded) {
      case (timeOfChange, (timestamp, contractsToAdd)) =>
        writeContractsAddedEvents(repair, hostedParties, timeOfChange.rc, timestamp, contractsToAdd)
    }

  private def packageVetted(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      packageDescription <- EitherTUtil.fromFuture(
        packageDependencyResolver.getPackageDescription(lfPackageId),
        t => log(s"Failed to look up package ${lfPackageId}", t),
      )
      _packageVetted <- EitherTUtil
        .condUnitET[Future](
          packageDescription.nonEmpty,
          log(s"Failed to locate package ${lfPackageId}"),
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
             |${domain.startingPoints.lastPublishedLocalOffset} offset inclusive
             |and the repair command would be assigned the offset ${domain.startingPoints.processing.nextRequestCounter}.
             |Reconnect to the domain to reprocess the dirty requests and retry repair afterwards.""".stripMargin
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
             |Reconnect to the domain to reprocess dirty requests and retry repair afterwards.""".stripMargin
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
          RequestState.Pending,
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

  private def readContractAcsState(persistentState: SyncDomainPersistentState, cid: LfContractId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, String, Option[ActiveContractStore.Status]] =
    EitherTUtil.fromFuture(
      persistentState.activeContractStore.fetchState(cid).map(_.map(_.status)),
      e => log(s"Failed to look up contract ${cid} status in ActiveContractStore: ${e}"),
    )

  private def readContractKey(
      domain: RepairRequest.DomainData,
      contract: SerializableContract,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[RepairService.KeyState]] =
    for {
      optionalKey <- getKeyIfOneMaintainerIsLocal(
        domain.topologySnapshot,
        contract.metadata.maybeKeyWithMaintainers,
        participantId,
      )
      optionalState <- optionalKey.flatTraverse(
        domain.persistentState.contractKeyJournal.fetchState
      )
    } yield optionalKey.map(RepairService.KeyState(_, optionalState))

  // Looks up domain persistence erroring if domain is based on in-memory persistence for which repair is not supported.
  private def lookUpDomainPersistence(domainId: DomainId, domainDescription: String)(implicit
      traceContext: TraceContext
  ): Either[String, SyncDomainPersistentState] =
    for {
      dp <- syncDomainPersistentStateManager
        .get(domainId)
        .toRight(log(s"Could not find ${domainDescription}"))
      _ <- Either.cond(
        !dp.isMemory(),
        (),
        log(
          s"${domainDescription} is in memory which is not supported by repair. Use db persistence."
        ),
      )
    } yield dp

  private def lockAndAwaitEitherT[B](description: String, code: => EitherT[Future, String, B])(
      implicit traceContext: TraceContext
  ): Either[String, B] = {
    logger.info(s"Queuing $description")
    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.await(description)(
      executionQueue
        .executeE(code, description)
        .value
        .onShutdown(Left(s"$description aborted due to shutdown"))
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
        createArguments: Record,
        signatories: Set[String],
        observers: Set[String],
        lfContractId: LfContractId,
        ledgerTime: Instant,
        contractSalt: Option[Salt],
    )(implicit namedLoggingContext: NamedLoggingContext): Either[String, SerializableContract] = {
      for {
        template <- LedgerApiFieldValidations.validateIdentifier(templateId).leftMap(_.getMessage)

        argsValue <- LedgerApiValueValidator
          .validateRecord(createArguments)
          .leftMap(e => s"Failed to validate arguments: ${e}")

        argsVersionedValue = LfVersioned(
          protocol.DummyTransactionVersion, // Version is ignored by daml engine upon RepairService.addContract
          argsValue,
        )

        lfContractInst = LfContractInst(template = template, arg = argsVersionedValue)

        /*
         It is fine to set the agreement text to empty because method `addContract` recomputes the agreement text
         and will discard this value.
         */
        serializableRawContractInst <- SerializableRawContractInstance
          .create(lfContractInst, AgreementText.empty)
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
      (Identifier, Record, Set[String], Set[String], LfContractId, Option[Salt], LedgerCreateTime),
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
      transferCounter: TransferCounterO,
      transferringFrom: Option[SourceDomainId],
      keyToAssign: Option[LfGlobalKey],
  ) {
    def driverMetadata(protocolVersion: ProtocolVersion): Bytes =
      contract.contractSalt
        .map(DriverContractMetadata(_).toLfBytes(protocolVersion))
        .getOrElse(Bytes.Empty)
  }

  private final case class KeyState(
      key: LfGlobalKey,
      state: Option[ContractKeyJournal.ContractKeyState],
  ) {
    def checkAssignable(domain: RepairRequest.DomainData): Either[String, LfGlobalKey] =
      Either.cond(
        !domain.parameters.uniqueContractKeys ||
          state.map(_.status).forall(_ == ContractKeyJournal.Unassigned),
        key,
        show"Key $key is already assigned to a different contract",
      )
  }

}
