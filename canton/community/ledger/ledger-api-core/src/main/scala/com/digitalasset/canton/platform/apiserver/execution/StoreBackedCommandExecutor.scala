// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.implicits.toBifunctorOps
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{
  Engine,
  Error as DamlLfError,
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
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.data.{CantonTimestamp, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.domain.{Commands as ApiCommands, DisclosedContract}
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
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.protocol.{
  AgreementText,
  ContractMetadata,
  DriverContractMetadata,
  SerializableContract,
}
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
)(implicit
    ec: ExecutionContext
) extends CommandExecutor
    with NamedLogging {
  private[this] val packageLoader = new DeduplicatingPackageLoader()

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
      _ <- Future.sequence(coids.map(contractStore.lookupContractStateWithoutDivulgence))
      submissionResult <- submitToEngine(commands, submissionSeed, interpretationTimeNanos)
      submission <- consume(
        commands.actAs,
        commands.readAs,
        submissionResult,
        commands.disclosedContracts.toList.map(c => c.contractId -> c).toMap,
        interpretationTimeNanos,
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
        .left
        .map(ErrorCause.DamlLf)
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
          commitAuthorizers,
          commands.readAs,
          commands.commands,
          commands.disclosedContracts.map(_.toLf),
          participant,
          submissionSeed,
        )
      }),
    )

  private def consume[A](
      actAs: Set[Ref.Party],
      readAs: Set[Ref.Party],
      result: Result[A],
      disclosedContracts: Map[ContractId, DisclosedContract],
      interpretationTimeNanos: AtomicLong,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Either[DamlLfError, A]] = {
    val readers = actAs ++ readAs

    val lookupActiveContractTime = new AtomicLong(0L)
    val lookupActiveContractCount = new AtomicLong(0L)

    val lookupContractKeyTime = new AtomicLong(0L)
    val lookupContractKeyCount = new AtomicLong(0L)

    // By unused here we mean they are not used by the verification
    val unusedTxVersion = TransactionVersion.StableVersions.max
    val unusedAgreementText = AgreementText.empty

    def extractSalt(dm: Array[Byte]): Either[String, Salt] =
      DriverContractMetadata
        .fromByteArray(dm)
        .bimap(
          e => s"Failed to build DriverContractMetadata ($e)",
          m => m.salt,
        )

    def extractOptSalt(driverMetadata: Option[Array[Byte]]): Either[String, Salt] =
      driverMetadata match {
        case Some(dm) => extractSalt(dm)
        case None => Left(s"Did not find salt")
      }

    def mapUpgradeResult(uvr: UpgradeVerificationResult): Option[String] = {
      import UpgradeVerificationResult.*
      uvr match {
        case Valid =>
          None
        case UpgradeFailure(message) =>
          Some(message)
        case DivulgedContract =>
          // During submission the ResultNeedUpgradeVerification should only be called
          // for contracts that are being upgraded. We do not support the upgrading of
          // divulged contracts.
          Some("Divulged contracts cannot be upgraded")
      }
    }

    def upgradableContractId(
        coid: ContractId,
        metadata: ContractMetadata,
    ): Future[Option[String]] = {
      val fResult = disclosedContracts.get(coid) match {
        case Some(contract) => upgradableDisclosedContract(contract, metadata)
        case None => upgradableNonDisclosedContract(coid, metadata)
      }
      fResult.map(mapUpgradeResult)
    }

    def upgradableDisclosedContract(
        contract: DisclosedContract,
        metadata: ContractMetadata,
    ): Future[UpgradeVerificationResult] = {

      import UpgradeVerificationResult.*

      val result: Either[String, SerializableContract] = for {
        salt <- extractSalt(contract.driverMetadata.toByteArray)
        contract <- SerializableContract(
          contractId = contract.contractId,
          contractInstance =
            Versioned(unusedTxVersion, ContractInstance(contract.templateId, contract.argument)),
          metadata = metadata,
          ledgerTime = CantonTimestamp(contract.createdAt),
          contractSalt = Some(salt),
          unvalidatedAgreementText = unusedAgreementText,
        ).left.map(e => s"Failed to construct SerializableContract($e)")
        _ <- authenticateContract(contract)
      } yield contract

      Future.successful(
        result.fold[UpgradeVerificationResult](message => UpgradeFailure(message), _ => Valid)
      )
    }

    def upgradableNonDisclosedContract(
        coid: ContractId,
        metadata: ContractMetadata,
    ): Future[UpgradeVerificationResult] = {
      import ContractState.*
      import UpgradeVerificationResult.*

      contractStore.lookupContractStateWithoutDivulgence(coid).flatMap {
        case active: ContractState.Active => upgradableActiveContract(coid, active, metadata)
        case Archived => Future.successful(UpgradeFailure("Contract archived"))
        case NotFound => Future.successful(DivulgedContract)
      }
    }

    def upgradableActiveContract(
        contractId: ContractId,
        active: ContractState.Active,
        metadata: ContractMetadata,
    ): Future[UpgradeVerificationResult] = {
      import UpgradeVerificationResult.*

      val result = for {
        salt <- extractOptSalt(active.driverMetadata)
        contract <- SerializableContract(
          contractId = contractId,
          contractInstance = active.contractInstance,
          metadata = metadata,
          ledgerTime = CantonTimestamp(active.ledgerEffectiveTime),
          contractSalt = Some(salt),
          unvalidatedAgreementText = unusedAgreementText,
        ).left.map(e => s"Failed to construct SerializableContract($e)")
        _ <- authenticateContract(contract)
      } yield ()

      Future.successful(
        result.fold[UpgradeVerificationResult](message => UpgradeFailure(message), _ => Valid)
      )

    }

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] =
      result match {
        case ResultDone(r) => Future.successful(Right(r))

        case ResultError(err) => Future.successful(Left(err))

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
          resolveStep(
            Tracked.value(
              metrics.daml.execution.engineRunning,
              trackSyncExecution(interpretationTimeNanos)(continue()),
            )
          )

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
          val metadata = ContractMetadata.tryCreate(
            signatories = signatories,
            stakeholders = signatories ++ observers,
            maybeKeyWithMaintainers = keyOpt.map(k => Versioned(unusedTxVersion, k)),
          )

          upgradableContractId(coid, metadata).flatMap(result => {
            resolveStep(
              Tracked.value(
                metrics.daml.execution.engineRunning,
                trackSyncExecution(interpretationTimeNanos)(resume(result)),
              )
            )
          })
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
}

object StoreBackedCommandExecutor {
  type AuthenticateContract = SerializableContract => Either[String, Unit]
}

private sealed trait UpgradeVerificationResult

private object UpgradeVerificationResult {
  case object Valid extends UpgradeVerificationResult
  final case class UpgradeFailure(message: String) extends UpgradeVerificationResult
  case object DivulgedContract extends UpgradeVerificationResult
}
