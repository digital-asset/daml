// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.*
import cats.syntax.all.*
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{
  Engine,
  Result,
  ResultDone,
  ResultError,
  ResultInterruption,
  ResultNeedAuthority,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
  ResultNeedUpgradeVerification,
}
import com.daml.lf.transaction.*
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import com.daml.metrics.{Timed, Tracked}
import com.digitalasset.canton.data.{CantonTimestamp, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.domain.{
  Commands as ApiCommands,
  DisclosedContract,
  NonUpgradableDisclosedContract,
  UpgradableDisclosedContract,
}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractState,
  ContractStore,
  IndexPackagesService,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateContract
import com.digitalasset.canton.platform.apiserver.execution.UpgradeVerificationResult.MissingDriverMetadata
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.protocol.{
  AgreementText,
  ContractMetadata,
  DriverContractMetadata,
  SerializableContract,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.Checked
import scalaz.syntax.tag.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

/** @param ec [[scala.concurrent.ExecutionContext]] that will be used for scheduling CPU-intensive computations
  *           performed by an [[com.daml.lf.engine.Engine]].
  */
private[apiserver] final class StoreBackedCommandExecutor(
    engine: Engine,
    participant: Ref.ParticipantId,
    packagesService: IndexPackagesService,
    contractStore: ContractStore,
    authorityResolver: AuthorityResolver,
    authenticateContract: AuthenticateContract,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
    dynParamGetter: DynamicDomainParameterGetter,
    timeProvider: TimeProvider,
)(implicit
    ec: ExecutionContext
) extends CommandExecutor
    with NamedLogging {
  private[this] val packageLoader = new DeduplicatingPackageLoader()
  // By unused here we mean that the TX version is not used by the verification
  private val unusedTxVersion = TransactionVersion.StableVersions.max

  override def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    val interpretationTimeNanos = new AtomicLong(0L)
    val start = System.nanoTime()
    val coids = commands.commands.commands.toSeq.foldLeft(Set.empty[Value.ContractId]) {
      case (acc, com.daml.lf.command.ApiCommand.Exercise(_, coid, _, argument)) =>
        argument.collectCids(acc) + coid
      case (acc, _) => acc
    }
    for {
      ledgerTimeRecordTimeToleranceO <- dynParamGetter
        // TODO(i15313):
        // We should really pass the domainId here, but it is not available within the ledger API for 2.x.
        .getLedgerTimeRecordTimeTolerance(None)
        .leftMap { error =>
          logger.info(
            s"Cannot retrieve ledgerTimeRecordTimeTolerance: $error. Command interpretation time will not be limited."
          )
        }
        .value
        .map(_.toOption)
      _ <- Future.sequence(coids.map(contractStore.lookupContractStateWithoutDivulgence))
      submissionResult <- submitToEngine(commands, submissionSeed, interpretationTimeNanos)
      submission <- consume(
        commands.actAs,
        commands.readAs,
        submissionResult,
        commands.disclosedContracts.toList.map(c => c.contractId -> c).toMap,
        interpretationTimeNanos,
        commands.commands.ledgerEffectiveTime,
        ledgerTimeRecordTimeToleranceO,
      )
    } yield {
      submission
        .map { case (updateTx, meta) =>
          val interpretationTimeNanos = System.nanoTime() - start
          commandExecutionResult(
            commands,
            submissionSeed,
            ledgerConfiguration,
            updateTx,
            meta,
            interpretationTimeNanos,
          )
        }
    }
  }

  private def commandExecutionResult(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
      updateTx: SubmittedTransaction,
      meta: Transaction.Metadata,
      interpretationTimeNanos: Long,
  ) = {
    val disclosedContractsMap =
      commands.disclosedContracts.toSeq.view.map(d => d.contractId -> d).toMap
    CommandExecutionResult(
      submitterInfo = state.SubmitterInfo(
        commands.actAs.toList,
        commands.readAs.toList,
        commands.applicationId,
        commands.commandId.unwrap,
        commands.deduplicationPeriod,
        commands.submissionId.map(_.unwrap),
        ledgerConfiguration,
      ),
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
        commands.domainId,
      ),
      transaction = updateTx,
      dependsOnLedgerTime = meta.dependsOnTime,
      interpretationTimeNanos = interpretationTimeNanos,
      globalKeyMapping = meta.globalKeyMapping,
      processedDisclosedContracts = meta.disclosedEvents.map { event =>
        val input = disclosedContractsMap(event.coid)
        ProcessedDisclosedContract(
          event,
          input.createdAt,
          input.driverMetadata,
        )
      },
    )
  }

  private def submitToEngine(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      interpretationTimeNanos: AtomicLong,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[(SubmittedTransaction, Transaction.Metadata)]] =
    Tracked.future(
      metrics.daml.execution.engineRunning,
      Future(trackSyncExecution(interpretationTimeNanos) {
        // The actAs and readAs parties are used for two kinds of checks by the ledger API server:
        // When looking up contracts during command interpretation, the engine should only see contracts
        // that are visible to at least one of the actAs or readAs parties. This visibility check is not part of the
        // Daml ledger model.
        // When checking Daml authorization rules, the engine verifies that the actAs parties are sufficient to
        // authorize the resulting transaction.
        val commitAuthorizers = commands.actAs
        engine.submit(
          submitters = commitAuthorizers,
          readAs = commands.readAs,
          cmds = commands.commands,
          disclosures = commands.disclosedContracts.map(_.toLf),
          participantId = participant,
          submissionSeed = submissionSeed,
        )
      }),
    )

  private def consume[A](
      actAs: Set[Ref.Party],
      readAs: Set[Ref.Party],
      result: Result[A],
      disclosedContracts: Map[ContractId, DisclosedContract],
      interpretationTimeNanos: AtomicLong,
      ledgerEffectiveTime: Time.Timestamp,
      ledgerTimeRecordTimeToleranceO: Option[NonNegativeFiniteDuration],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Either[ErrorCause, A]] = {
    val readers = actAs ++ readAs

    val lookupActiveContractTime = new AtomicLong(0L)
    val lookupActiveContractCount = new AtomicLong(0L)

    val lookupContractKeyTime = new AtomicLong(0L)
    val lookupContractKeyCount = new AtomicLong(0L)

    def resolveStep(result: Result[A]): Future[Either[ErrorCause, A]] =
      result match {
        case ResultDone(r) => Future.successful(Right(r))

        case ResultError(err) => Future.successful(Left(ErrorCause.DamlLf(err)))

        case ResultNeedContract(acoid, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupActiveContract,
              contractStore.lookupActiveContract(readers, acoid),
            )
            .flatMap { instance =>
              lookupActiveContractTime.addAndGet(System.nanoTime() - start)
              lookupActiveContractCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(instance)),
                )
              )
            }

        case ResultNeedKey(key, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupContractKey,
              contractStore.lookupContractKey(readers, key.globalKey),
            )
            .flatMap { contractId =>
              lookupContractKeyTime.addAndGet(System.nanoTime() - start)
              lookupContractKeyCount.incrementAndGet()
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(contractId)),
                )
              )
            }

        case ResultNeedPackage(packageId, resume) =>
          packageLoader
            .loadPackage(
              packageId = packageId,
              delegate = packageId => packagesService.getLfArchive(packageId)(loggingContext),
              metric = metrics.daml.execution.getLfPackage,
            )
            .flatMap { maybePackage =>
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(maybePackage)),
                )
              )
            }

        case ResultInterruption(continue) =>
          // We want to prevent the interpretation to run indefinitely and use all the resources.
          // For this purpose, we check the following condition:
          //
          //    Ledger Effective Time + skew > wall clock
          //
          // The skew is given by the dynamic domain parameter `ledgerTimeRecordTimeTolerance`.
          //
          // As defined in the "Time on Daml Ledgers" chapter of the documentation, if this condition
          // is true, then the Record Time (assigned later on when the transaction is sequenced) is already
          // out of bounds, and the sequencer will reject the transaction. We can therefore abort the
          // interpretation and return an error to the application.
          def resume(): Future[Either[ErrorCause, A]] =
            resolveStep(
              Tracked.value(
                metrics.daml.execution.engineRunning,
                trackSyncExecution(interpretationTimeNanos)(continue()),
              )
            )

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
                  )
                Future.successful(Left(error))
              } else resume()
          }

        case ResultNeedAuthority(holding @ _, requesting @ _, resume) =>
          authorityResolver
            // TODO(i12742) DomainId is required to be passed here
            .resolve(AuthorityResolver.AuthorityRequest(holding, requesting, domainId = None))
            .flatMap { response =>
              val resumed = response match {
                case AuthorityResolver.AuthorityResponse.MissingAuthorisation(parties) =>
                  val receivedAuthorityFor = (parties -- requesting).mkString(",")
                  val missingAuthority = parties.mkString(",")
                  logger.debug(
                    s"Authorisation failed. Missing authority: [$missingAuthority]. Received authority for: [$receivedAuthorityFor]"
                  )
                  false
                case AuthorityResolver.AuthorityResponse.Authorized =>
                  true
              }
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(resumed)),
                )
              )
            }

        case ResultNeedUpgradeVerification(coid, signatories, observers, keyOpt, resume) =>
          checkContractUpgradable(coid, signatories, observers, keyOpt, disclosedContracts).flatMap(
            result => {
              resolveStep(
                Tracked.value(
                  metrics.daml.execution.engineRunning,
                  trackSyncExecution(interpretationTimeNanos)(resume(result)),
                )
              )
            }
          )
      }

    resolveStep(result).andThen { case _ =>
      metrics.daml.execution.lookupActiveContractPerExecution
        .update(lookupActiveContractTime.get(), TimeUnit.NANOSECONDS)
      metrics.daml.execution.lookupActiveContractCountPerExecution
        .update(lookupActiveContractCount.get)
      metrics.daml.execution.lookupContractKeyPerExecution
        .update(lookupContractKeyTime.get(), TimeUnit.NANOSECONDS)
      metrics.daml.execution.lookupContractKeyCountPerExecution
        .update(lookupContractKeyCount.get())
      metrics.daml.execution.engine
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
      disclosedContracts: Map[ContractId, DisclosedContract],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[String]] = {

    val stakeholders = signatories ++ observers
    val maybeKeyWithMaintainers = keyWithMaintainers.map(Versioned(unusedTxVersion, _))
    ContractMetadata.create(
      signatories = signatories,
      stakeholders = stakeholders,
      maybeKeyWithMaintainers = maybeKeyWithMaintainers,
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
      disclosedContracts: Map[ContractId, DisclosedContract],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[String]] = {
    import UpgradeVerificationResult.*
    type Result = EitherT[Future, UpgradeVerificationResult, UpgradeVerificationContractData]

    def checkProvidedContractMetadataAgainstRecomputed(
        original: ContractMetadata,
        recomputed: ContractMetadata,
    ): Either[NonEmptyChain[String], Unit] = {
      def check[T](recomputed: T, original: T)(desc: String): Checked[Nothing, String, Unit] =
        Checked.fromEitherNonabort(())(
          Either.cond(recomputed == original, (), s"$desc mismatch: $original vs $recomputed")
        )

      for {
        _ <- check(recomputed.signatories, original.signatories)("signatories")
        recomputedObservers = recomputed.stakeholders -- recomputed.signatories
        originalObservers = original.stakeholders -- original.signatories
        _ <- check(recomputedObservers, originalObservers)("observers")
        _ <- check(recomputed.maintainers, original.maintainers)("key maintainers")
        _ <- check(recomputed.maybeKey, original.maybeKey)("key value")
      } yield ()
    }.toEitherMergeNonaborts

    def validateContractAuthentication(
        upgradeVerificationContractData: UpgradeVerificationContractData
    ): Future[UpgradeVerificationResult] = {
      import upgradeVerificationContractData.*

      val result: Either[String, SerializableContract] = for {
        salt <- DriverContractMetadata
          .fromByteArray(driverMetadataBytes)
          .bimap(
            e => s"Failed to build DriverContractMetadata ($e)",
            m => m.salt,
          )
        contract <- SerializableContract(
          contractId = contractId,
          contractInstance = contractInstance,
          metadata = recomputedMetadata,
          ledgerTime = ledgerTime,
          contractSalt = Some(salt),
          // The agreement text is unused on contract authentication
          unvalidatedAgreementText = AgreementText.empty,
        ).left.map(e => s"Failed to construct SerializableContract($e)")
        _ <- authenticateContract(contract).leftMap { contractAuthenticationError =>
          val firstParticle =
            s"Upgrading contract with ${upgradeVerificationContractData.contractId} failed authentication check with error: $contractAuthenticationError."
          checkProvidedContractMetadataAgainstRecomputed(originalMetadata, recomputedMetadata)
            .leftMap(_.mkString_("['", "', '", "']"))
            .fold(
              value => s"$firstParticle The following upgrading checks failed: $value",
              _ => firstParticle,
            )
        }
      } yield contract

      EitherT.fromEither[Future](result).fold(UpgradeFailure, _ => Valid)
    }

    // TODO(#14884): Guard contract activeness check with readers permission check
    def lookupActiveContractVerificationData(): Result =
      EitherT(
        contractStore
          .lookupContractStateWithoutDivulgence(coid)
          .map {
            case active: ContractState.Active =>
              UpgradeVerificationContractData
                .fromActiveContract(coid, active, recomputedContractMetadata)
            case ContractState.Archived => Left(UpgradeFailure("Contract archived"))
            case ContractState.NotFound => Left(ContractNotFound)
          }
      )

    val checkDisclosedContractUpgradable: DisclosedContract => Result = {
      case upgradable: UpgradableDisclosedContract =>
        EitherT.rightT(
          UpgradeVerificationContractData.fromDisclosedContract(
            upgradable,
            recomputedContractMetadata,
          )
        )
      // Upgrading is not supported for deprecated disclosed contract format
      case _: NonUpgradableDisclosedContract =>
        EitherT.leftT(DeprecatedDisclosedContractFormat: UpgradeVerificationResult)
    }

    val handleVerificationResult: UpgradeVerificationResult => Option[String] = { result =>
      val response: Option[String] = result match {
        case Valid => None
        case UpgradeFailure(message) => Some(message)
        case ContractNotFound =>
          // During submission the ResultNeedUpgradeVerification should only be called
          // for contracts that are being upgraded. We do not support the upgrading of
          // divulged contracts.
          Some(s"Contract with $coid was not found or it refers to a divulged contract.")
        case MissingDriverMetadata =>
          Some(
            s"Contract with $coid is missing the driver metadata and cannot be upgraded. This can happen for contracts created with older Canton versions"
          )
        case DeprecatedDisclosedContractFormat =>
          Some(
            s"Contract with $coid was provided via the deprecated DisclosedContract create_argument_blob field and cannot be upgraded. Use the create_argument_payload instead and retry the submission"
          )
      }
      response.foreach(message => logger.info(message))
      response
    }

    disclosedContracts
      .get(coid)
      .map(checkDisclosedContractUpgradable)
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
  )

  private object UpgradeVerificationContractData {
    def fromDisclosedContract(
        disclosedContract: UpgradableDisclosedContract,
        recomputedMetadata: ContractMetadata,
    ): UpgradeVerificationContractData =
      UpgradeVerificationContractData(
        contractId = disclosedContract.contractId,
        driverMetadataBytes = disclosedContract.driverMetadata.toByteArray,
        contractInstance = Versioned(
          unusedTxVersion,
          ContractInstance(
            template = disclosedContract.templateId,
            arg = disclosedContract.argument,
          ),
        ),
        originalMetadata = ContractMetadata.tryCreate(
          signatories = disclosedContract.signatories,
          stakeholders = disclosedContract.stakeholders,
          maybeKeyWithMaintainers =
            (disclosedContract.keyValue zip disclosedContract.keyMaintainers).map {
              case (value, maintainers) =>
                val sharedKey = recomputedMetadata.maybeKey.forall(GlobalKey.isShared)
                Versioned(
                  unusedTxVersion,
                  GlobalKeyWithMaintainers
                    .assertBuild(disclosedContract.templateId, value, maintainers, sharedKey),
                )
            },
        ),
        recomputedMetadata = recomputedMetadata,
        ledgerTime = CantonTimestamp(disclosedContract.createdAt),
      )

    def fromActiveContract(
        contractId: ContractId,
        active: ContractState.Active,
        recomputedMetadata: ContractMetadata,
    ): Either[MissingDriverMetadata.type, UpgradeVerificationContractData] =
      active.driverMetadata
        .toRight(MissingDriverMetadata)
        .map { driverMetadataBytes =>
          UpgradeVerificationContractData(
            contractId = contractId,
            driverMetadataBytes = driverMetadataBytes,
            contractInstance = active.contractInstance,
            originalMetadata = ContractMetadata.tryCreate(
              signatories = active.signatories,
              stakeholders = active.stakeholders,
              maybeKeyWithMaintainers =
                (active.globalKey zip active.maintainers).map { case (globalKey, maintainers) =>
                  Versioned(unusedTxVersion, GlobalKeyWithMaintainers(globalKey, maintainers))
                },
            ),
            recomputedMetadata = recomputedMetadata,
            ledgerTime = CantonTimestamp(active.ledgerEffectiveTime),
          )
        }
  }
}

object StoreBackedCommandExecutor {
  type AuthenticateContract = SerializableContract => Either[String, Unit]
}

private sealed trait UpgradeVerificationResult extends Product with Serializable

private object UpgradeVerificationResult {
  case object Valid extends UpgradeVerificationResult
  final case class UpgradeFailure(message: String) extends UpgradeVerificationResult
  case object ContractNotFound extends UpgradeVerificationResult
  case object DeprecatedDisclosedContractFormat extends UpgradeVerificationResult
  case object MissingDriverMetadata extends UpgradeVerificationResult
}
