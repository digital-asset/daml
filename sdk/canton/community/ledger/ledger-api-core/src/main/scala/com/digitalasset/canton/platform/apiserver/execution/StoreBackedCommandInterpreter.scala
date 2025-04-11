// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.*
import cats.syntax.all.*
import com.daml.metrics.{Timed, Tracked}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.Commands as ApiCommands
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.PackageSyncService
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStore}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  DriverContractMetadata,
  SerializableContract,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Checked
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractId, ThinContractInstance}
import scalaz.syntax.tag.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.collection.View
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] trait CommandInterpreter {

  def interpret(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Either[ErrorCause, CommandInterpretationResult]]
}

/** @param ec
  *   [[scala.concurrent.ExecutionContext]] that will be used for scheduling CPU-intensive
  *   computations performed by an [[com.digitalasset.daml.lf.engine.Engine]].
  */
final class StoreBackedCommandInterpreter(
    engine: Engine,
    participant: Ref.ParticipantId,
    packageSyncService: PackageSyncService,
    contractStore: ContractStore,
    metrics: LedgerApiServerMetrics,
    authenticateSerializableContract: SerializableContract => Either[String, Unit],
    config: EngineLoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    dynParamGetter: DynamicSynchronizerParameterGetter,
    timeProvider: TimeProvider,
)(implicit
    ec: ExecutionContext
) extends CommandInterpreter
    with NamedLogging {
  private[this] val packageLoader = new DeduplicatingPackageLoader()
  // By unused here we mean that the TX version is not used by the verification
  private val unusedTxVersion = LanguageVersion.StableVersions(LanguageVersion.Major.V2).max

  override def interpret(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Either[ErrorCause, CommandInterpretationResult]] = {
    val interpretationTimeNanos = new AtomicLong(0L)
    val start = System.nanoTime()
    for {
      ledgerTimeRecordTimeToleranceO <- dynParamGetter
        // TODO(i15313):
        // We should really pass the synchronizerId here, but it is not available within the ledger API for 2.x.
        .getLedgerTimeRecordTimeTolerance(None)
        .leftMap { error =>
          logger.info(
            s"Cannot retrieve ledgerTimeRecordTimeTolerance: $error. Command interpretation time will not be limited."
          )
        }
        .value
        .map(_.toOption)
      submissionResult <- submitToEngine(commands, submissionSeed, interpretationTimeNanos)
      submission <- consume(
        commands.actAs,
        commands.readAs,
        submissionResult,
        commands.disclosedContracts.iterator
          .map(c => c.fatContractInstance.contractId -> c.fatContractInstance)
          .toMap,
        interpretationTimeNanos,
        commands.commands.ledgerEffectiveTime,
        ledgerTimeRecordTimeToleranceO,
      )

    } yield submission.flatMap { case (updateTx, meta) =>
      val interpretationTimeNanos = System.nanoTime() - start
      commandInterpretationResult(
        commands,
        submissionSeed,
        updateTx,
        meta,
        interpretationTimeNanos,
      )
    }
  }

  private def commandInterpretationResult(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      updateTx: SubmittedTransaction,
      meta: Transaction.Metadata,
      interpretationTimeNanos: Long,
  )(implicit
      tc: TraceContext
  ): Either[ErrorCause.DisclosedContractsSynchronizerIdMismatch, CommandInterpretationResult] = {
    val disclosedContractsMap =
      commands.disclosedContracts.iterator.map(d => d.fatContractInstance.contractId -> d).toMap

    val processedDisclosedContractsSynchronizers = meta.disclosedEvents
      .map { event =>
        val disclosedContract = disclosedContractsMap(event.coid)
        disclosedContract.fatContractInstance -> disclosedContract.synchronizerIdO
      }

    StoreBackedCommandInterpreter
      .considerDisclosedContractsSynchronizerId(
        commands.synchronizerId,
        processedDisclosedContractsSynchronizers.map { case (disclosed, synchronizerIdO) =>
          disclosed.contractId -> synchronizerIdO
        },
        logger,
      )
      .map { prescribedSynchronizerIdO =>
        CommandInterpretationResult(
          submitterInfo = state.SubmitterInfo(
            commands.actAs.toList,
            commands.readAs.toList,
            commands.userId,
            commands.commandId.unwrap,
            commands.deduplicationPeriod,
            commands.submissionId.map(_.unwrap),
            externallySignedSubmission = None,
          ),
          optSynchronizerId = prescribedSynchronizerIdO,
          transactionMeta = state.TransactionMeta(
            commands.commands.ledgerEffectiveTime,
            commands.workflowId.map(_.unwrap),
            meta.submissionTime,
            submissionSeed,
            Some(meta.usedPackages),
            Some(meta.nodeSeeds),
            Some(
              updateTx.nodes
                .collect { case (nodeId, node: Node.Action) if node.byKey => nodeId }
                .to(ImmArray)
            ),
          ),
          transaction = updateTx,
          dependsOnLedgerTime = meta.dependsOnTime,
          interpretationTimeNanos = interpretationTimeNanos,
          globalKeyMapping = meta.globalKeyMapping,
          processedDisclosedContracts = processedDisclosedContractsSynchronizers.map(_._1),
        )
      }
  }

  private def submitToEngine(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      interpretationTimeNanos: AtomicLong,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Result[(SubmittedTransaction, Transaction.Metadata)]] =
    Tracked.future(
      metrics.execution.engineRunning,
      FutureUnlessShutdown.outcomeF(Future(trackSyncExecution(interpretationTimeNanos) {
        // The actAs and readAs parties are used for two kinds of checks by the ledger API server:
        // When looking up contracts during command interpretation, the engine should only see contracts
        // that are visible to at least one of the actAs or readAs parties. This visibility check is not part of the
        // Daml ledger model.
        // When checking Daml authorization rules, the engine verifies that the actAs parties are sufficient to
        // authorize the resulting transaction.
        val commitAuthorizers = commands.actAs
        engine.submit(
          packageMap = commands.packageMap,
          packagePreference = commands.packagePreferenceSet,
          submitters = commitAuthorizers,
          readAs = commands.readAs,
          cmds = commands.commands,
          disclosures = commands.disclosedContracts.map(_.fatContractInstance),
          participantId = participant,
          submissionSeed = submissionSeed,
          prefetchKeys = commands.prefetchKeys,
          config.toEngineLogger(loggerFactory.append("phase", "submission")),
        )
      })),
    )

  private def consume[A](
      actAs: Set[Ref.Party],
      readAs: Set[Ref.Party],
      result: Result[A],
      disclosedContracts: Map[ContractId, FatContractInstance],
      interpretationTimeNanos: AtomicLong,
      ledgerEffectiveTime: Time.Timestamp,
      ledgerTimeRecordTimeToleranceO: Option[NonNegativeFiniteDuration],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Either[ErrorCause, A]] = {
    val readers = actAs ++ readAs

    val lookupActiveContractTime = new AtomicLong(0L)
    val lookupActiveContractCount = new AtomicLong(0L)

    val lookupContractKeyTime = new AtomicLong(0L)
    val lookupContractKeyCount = new AtomicLong(0L)

    def resolveStep(result: Result[A]): FutureUnlessShutdown[Either[ErrorCause, A]] =
      result match {
        case ResultDone(r) => FutureUnlessShutdown.pure(Right(r))

        case ResultError(err) => FutureUnlessShutdown.pure(Left(ErrorCause.DamlLf(err)))

        case ResultNeedContract(acoid, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.execution.lookupActiveContract,
              FutureUnlessShutdown.outcomeF(contractStore.lookupActiveContract(readers, acoid)),
            )
            .flatMap { instance =>
              lookupActiveContractTime.addAndGet(System.nanoTime() - start)
              lookupActiveContractCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(instance)),
                )
              )
            }

        case ResultNeedKey(key, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.execution.lookupContractKey,
              FutureUnlessShutdown.outcomeF(contractStore.lookupContractKey(readers, key.globalKey)),
            )
            .flatMap { contractId =>
              lookupContractKeyTime.addAndGet(System.nanoTime() - start)
              lookupContractKeyCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(contractId)),
                )
              )
            }

        case ResultNeedPackage(packageId, resume) =>
          FutureUnlessShutdown
            .outcomeF(
              packageLoader
                .loadPackage(
                  packageId = packageId,
                  delegate = packageSyncService.getLfArchive(_)(loggingContext.traceContext),
                  metric = metrics.execution.getLfPackage,
                )
            )
            .flatMap { maybePackage =>
              resolveStep(
                Tracked.value(
                  metrics.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(maybePackage)),
                )
              )
            }

        case ResultInterruption(continue, abort) =>
          // We want to prevent the interpretation to run indefinitely and use all the resources.
          // For this purpose, we check the following condition:
          //
          //    Ledger Effective Time + skew > wall clock
          //
          // The skew is given by the dynamic synchronizer parameter `ledgerTimeRecordTimeTolerance`.
          //
          // As defined in the "Time on Daml Ledgers" chapter of the documentation, if this condition
          // is true, then the Record Time (assigned later on when the transaction is sequenced) is already
          // out of bounds, and the sequencer will reject the transaction. We can therefore abort the
          // interpretation and return an error to the application.

          // Using a `Future` as a trampoline to make the recursive call to `resolveStep` stack safe.
          def resume(): FutureUnlessShutdown[Either[ErrorCause, A]] =
            FutureUnlessShutdown
              .outcomeF {
                Future {
                  Tracked.value(
                    metrics.execution.engineRunning,
                    trackSyncExecution(interpretationTimeNanos)(continue()),
                  )
                }
              }
              .flatMap(resolveStep)

          ledgerTimeRecordTimeToleranceO match {
            // Fall back to not checking if the tolerance could not be retrieved
            case None => resume()

            case Some(ledgerTimeRecordTimeTolerance) =>
              val let = ledgerEffectiveTime.toInstant
              val currentTime = timeProvider.getCurrentTime

              val limitExceeded =
                currentTime.isAfter(let.plus(ledgerTimeRecordTimeTolerance.duration))

              if (limitExceeded) {
                val error: ErrorCause = ErrorCause
                  .InterpretationTimeExceeded(
                    ledgerEffectiveTime,
                    ledgerTimeRecordTimeTolerance,
                    abort(),
                  )
                FutureUnlessShutdown.pure(Left(error))
              } else resume()
          }

        case ResultNeedUpgradeVerification(coid, signatories, observers, keyOpt, resume) =>
          FutureUnlessShutdown
            .outcomeF(
              checkContractUpgradable(coid, signatories, observers, keyOpt, disclosedContracts)
            )
            .flatMap { result =>
              resolveStep(
                Tracked.value(
                  metrics.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(result)),
                )
              )
            }

        case ResultPrefetch(coids, keys, resume) =>
          // Trigger loading through the state cache and the batch aggregator.
          // Loading of contracts is a multi-stage process.
          // - start with N items
          // - trigger a single load in contractStore (1:1)
          // - visit the mutableStateCache which will use the read through lookup
          // - the read through lookup will ask the contract reader
          // - the contract reader will ask the batchLoader
          // - the batch loader will put independent requests together into db batches and respond
          val loadContractsF = Future.sequence(coids.map(contractStore.lookupContractState))
          // prefetch the contract keys via the mutable state cache / batch aggregator
          // then prefetch the found contracts in the same way
          val loadKeysF = {
            import com.digitalasset.canton.util.FutureInstances.*
            keys
              .parTraverse(key => contractStore.lookupContractKey(Set.empty, key))
              .flatMap(contractIds =>
                contractIds.flattenOption
                  .parTraverse_(contractId => contractStore.lookupContractState(contractId))
              )
          }
          FutureUnlessShutdown
            .outcomeF(loadContractsF.zip(loadKeysF))
            .flatMap(_ => resolveStep(resume()))
      }

    resolveStep(result).thereafter { _ =>
      metrics.execution.lookupActiveContractPerExecution
        .update(lookupActiveContractTime.get(), TimeUnit.NANOSECONDS)
      metrics.execution.lookupActiveContractCountPerExecution
        .update(lookupActiveContractCount.get)
      metrics.execution.lookupContractKeyPerExecution
        .update(lookupContractKeyTime.get(), TimeUnit.NANOSECONDS)
      metrics.execution.lookupContractKeyCountPerExecution
        .update(lookupContractKeyCount.get())
      metrics.execution.engine
        .update(interpretationTimeNanos.get(), TimeUnit.NANOSECONDS)
    }
  }

  private def trackSyncExecution[T](atomicNano: AtomicLong)(computation: => T): T = {
    val start = System.nanoTime()
    val result = computation
    atomicNano.addAndGet(System.nanoTime() - start)
    result
  }

  private def checkContractUpgradable(
      coid: ContractId,
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      keyWithMaintainers: Option[GlobalKeyWithMaintainers],
      disclosedContracts: Map[ContractId, FatContractInstance],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[String]] = {

    val stakeholders = signatories ++ observers
    val maybeKeyWithMaintainers = keyWithMaintainers.map(Versioned(unusedTxVersion, _))
    ContractMetadata.create(
      signatories = signatories,
      stakeholders = stakeholders,
      maybeKeyWithMaintainersVersioned = maybeKeyWithMaintainers,
    ) match {
      case Right(recomputedContractMetadata) =>
        checkContractUpgradable(coid, recomputedContractMetadata, disclosedContracts)
      case Left(message) =>
        val enriched =
          s"Failed to recompute contract metadata from ($signatories, $stakeholders, $maybeKeyWithMaintainers): $message"
        logger.info(enriched)
        Future.successful(Some(enriched))
    }

  }

  private def checkContractUpgradable(
      coid: ContractId,
      recomputedContractMetadata: ContractMetadata,
      disclosedContracts: Map[ContractId, FatContractInstance],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[String]] = {

    import UpgradeVerificationResult.*

    type Result = EitherT[Future, UpgradeVerificationResult, UpgradeVerificationContractData]

    def validateContractAuthentication(
        data: UpgradeVerificationContractData
    ): Future[UpgradeVerificationResult] =
      Future.successful(data.validate.fold(s => UpgradeFailure(s), _ => Valid))

    def lookupActiveContractVerificationData(): Result =
      EitherT(
        contractStore
          .lookupContractState(coid)
          .map {
            case active: ContractState.Active =>
              Right(
                UpgradeVerificationContractData
                  .fromActiveContract(coid, active, recomputedContractMetadata)
              )
            case ContractState.Archived | ContractState.NotFound => Left(ContractNotFound)
          }
      )

    val handleVerificationResult: UpgradeVerificationResult => Option[String] = {
      case Valid => None
      case UpgradeFailure(message) => Some(message)
      case ContractNotFound =>
        // During submission the ResultNeedUpgradeVerification should only be called
        // for contracts that are being upgraded. We do not support the upgrading of
        // divulged contracts.
        Some(s"Contract with $coid was not found.")
      case MissingDriverMetadata =>
        Some(
          s"Contract with $coid is missing the driver metadata and cannot be upgraded. This can happen for contracts created with older Canton versions"
        )
    }

    disclosedContracts
      .get(coid)
      .map { disclosedContract =>
        EitherT.rightT[Future, UpgradeVerificationResult](
          UpgradeVerificationContractData.fromDisclosedContract(
            disclosedContract,
            recomputedContractMetadata,
          )
        )
      }
      .getOrElse(lookupActiveContractVerificationData())
      .semiflatMap(validateContractAuthentication)
      .merge
      .map(handleVerificationResult)
  }

  private case class UpgradeVerificationContractData(
      contractId: ContractId,
      driverMetadataBytes: Array[Byte],
      contractInstance: Value.VersionedContractInstance,
      originalMetadata: ContractMetadata,
      recomputedMetadata: ContractMetadata,
      ledgerTime: CantonTimestamp,
  ) {
    private def recomputedSerializableContract: Either[String, SerializableContract] =
      for {
        salt <- DriverContractMetadata
          .fromTrustedByteArray(driverMetadataBytes)
          .bimap(
            e => s"Failed to build DriverContractMetadata ($e)",
            m => m.salt,
          )
        contract <- SerializableContract(
          contractId = contractId,
          contractInstance = contractInstance,
          metadata = recomputedMetadata,
          ledgerTime = ledgerTime,
          contractSalt = salt,
        ).left.map(e => s"Failed to construct SerializableContract($e)")
      } yield contract

    private def checkProvidedContractMetadataAgainstRecomputed
        : Either[NonEmptyChain[String], Unit] = {

      def check[T](f: ContractMetadata => T)(desc: String): Checked[Nothing, String, Unit] = {
        val original = f(originalMetadata)
        val recomputed = f(recomputedMetadata)
        Checked.fromEitherNonabort(())(
          Either.cond(recomputed == original, (), s"$desc mismatch: $original vs $recomputed")
        )
      }

      for {
        _ <- check(_.signatories)("signatories")
        _ <- check(m => m.stakeholders -- m.signatories)("observers")
        _ <- check(_.maintainers)("key maintainers")
        _ <- check(_.maybeKey)("key value")
      } yield ()

    }.toEitherMergeNonaborts

    def validate: Either[String, Unit] =
      (for {
        sc <- recomputedSerializableContract
        _ <- authenticateSerializableContract(sc)
      } yield ()).leftMap { contractAuthenticationError =>
        val firstParticle =
          s"Upgrading contract with $contractId failed authentication check with error: $contractAuthenticationError."
        checkProvidedContractMetadataAgainstRecomputed
          .leftMap(_.mkString_("['", "', '", "']"))
          .fold(
            value => s"$firstParticle The following upgrading checks failed: $value",
            _ => firstParticle,
          )
      }

  }

  private object UpgradeVerificationContractData {
    def fromDisclosedContract(
        disclosedContract: FatContractInstance,
        recomputedMetadata: ContractMetadata,
    ): UpgradeVerificationContractData =
      UpgradeVerificationContractData(
        contractId = disclosedContract.contractId,
        driverMetadataBytes = disclosedContract.cantonData.toByteArray,
        contractInstance = Versioned(
          disclosedContract.version,
          ThinContractInstance(
            packageName = disclosedContract.packageName,
            template = disclosedContract.templateId,
            arg = disclosedContract.createArg,
          ),
        ),
        originalMetadata = ContractMetadata.tryCreate(
          signatories = disclosedContract.signatories,
          stakeholders = disclosedContract.stakeholders,
          maybeKeyWithMaintainersVersioned = disclosedContract.contractKeyWithMaintainers.map(
            Versioned(disclosedContract.version, _)
          ),
        ),
        recomputedMetadata = recomputedMetadata,
        ledgerTime = CantonTimestamp(disclosedContract.createdAt),
      )

    def fromActiveContract(
        contractId: ContractId,
        active: ContractState.Active,
        recomputedMetadata: ContractMetadata,
    ): UpgradeVerificationContractData =
      UpgradeVerificationContractData(
        contractId = contractId,
        driverMetadataBytes = active.driverMetadata,
        contractInstance = active.contractInstance,
        originalMetadata = ContractMetadata.tryCreate(
          signatories = active.signatories,
          stakeholders = active.stakeholders,
          maybeKeyWithMaintainersVersioned =
            (active.globalKey zip active.maintainers).map { case (globalKey, maintainers) =>
              Versioned(
                active.contractInstance.version,
                GlobalKeyWithMaintainers(globalKey, maintainers),
              )
            },
        ),
        recomputedMetadata = recomputedMetadata,
        ledgerTime = CantonTimestamp(active.ledgerEffectiveTime),
      )
  }
}

object StoreBackedCommandInterpreter {

  def considerDisclosedContractsSynchronizerId(
      prescribedSynchronizerIdO: Option[SynchronizerId],
      disclosedContractsUsedInInterpretation: ImmArray[(ContractId, Option[SynchronizerId])],
      logger: TracedLogger,
  )(implicit
      tc: TraceContext
  ): Either[ErrorCause.DisclosedContractsSynchronizerIdMismatch, Option[SynchronizerId]] = {
    val disclosedContractsSynchronizerIds: View[(ContractId, SynchronizerId)] =
      disclosedContractsUsedInInterpretation.toSeq.view.collect {
        case (contractId, Some(synchronizerId)) => contractId -> synchronizerId
      }

    val synchronizerIdsOfDisclosedContracts = disclosedContractsSynchronizerIds.map(_._2).toSet
    if (synchronizerIdsOfDisclosedContracts.sizeIs > 1) {
      // Reject on diverging synchronizer ids for used disclosed contracts
      Left(
        ErrorCause.DisclosedContractsSynchronizerIdsMismatch(
          disclosedContractsSynchronizerIds.toMap
        )
      )
    } else
      disclosedContractsSynchronizerIds.headOption match {
        case None =>
          // If no disclosed contracts with a specified synchronizer id, use the prescribed one (if specified)
          Right(prescribedSynchronizerIdO)
        case Some((_, synchronizerIdOfDisclosedContracts)) =>
          prescribedSynchronizerIdO
            .map {
              // Both prescribed and from disclosed contracts synchronizer id - check for equality
              case prescribed if synchronizerIdOfDisclosedContracts == prescribed =>
                Right(Some(prescribed))
              case mismatchingPrescribed =>
                Left(
                  ErrorCause.PrescribedSynchronizerIdMismatch(
                    disclosedContractIds = disclosedContractsSynchronizerIds.map(_._1).toSet,
                    synchronizerIdOfDisclosedContracts = synchronizerIdOfDisclosedContracts,
                    commandsSynchronizerId = mismatchingPrescribed,
                  )
                )
            }
            // If the prescribed synchronizer id is not specified, use the synchronizer id of the disclosed contracts
            .getOrElse {
              logger.debug(
                s"Using the synchronizer id ($synchronizerIdOfDisclosedContracts) of the disclosed contracts used in command interpretation (${disclosedContractsSynchronizerIds
                    .map(_._1)
                    .mkString("[", ",", "]")}) as the prescribed synchronizer id."
              )
              Right(Some(synchronizerIdOfDisclosedContracts))
            }
      }
  }
}

private sealed trait UpgradeVerificationResult extends Product with Serializable

private object UpgradeVerificationResult {
  case object Valid extends UpgradeVerificationResult
  final case class UpgradeFailure(message: String) extends UpgradeVerificationResult
  case object ContractNotFound extends UpgradeVerificationResult
  case object MissingDriverMetadata extends UpgradeVerificationResult
}
