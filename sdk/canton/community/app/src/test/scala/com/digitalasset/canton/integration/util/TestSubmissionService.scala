// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction.TransactionTree
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  Filters,
  TransactionFilter,
  WildcardFilter,
}
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.UpdateTreeWrapper
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.{DeduplicationPeriod, LedgerTimeBoundaries}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.util.TestSubmissionService.{
  CommandsWithMetadata,
  TestKeyResolver,
}
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  ValidateDisclosedContracts,
  ValidateUpgradingPackageResolutions,
}
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.participant.util.DAMLe.PackageResolver
import com.digitalasset.canton.platform.apiserver.SeedService.WeakRandom
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.daml.lf.command.ApiCommands
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.{CommandId, SubmissionId, UserId, WorkflowId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.{
  Engine,
  EngineConfig,
  Error,
  Result,
  ResultDone,
  ResultError,
  ResultInterruption,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
  ResultNeedUpgradeVerification,
  ResultPrefetch,
}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.*
import io.grpc.stub.StreamObserver
import org.scalatest.OptionValues.*

import java.time.Duration
import java.util.UUID
import scala.annotation.{tailrec, unused}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class TestSubmissionService(
    participantId: ParticipantId,
    maxDeduplicationDuration: NonNegativeFiniteDuration,
    damle: Engine,
    contractResolver: LfContractId => TraceContext => Future[Option[FatContractInstance]],
    keyResolver: TestKeyResolver,
    packageResolver: PackageResolver,
    syncService: SyncService,
    basePackageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  implicit lazy val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  def submitAndWaitForTransactionTree(
      submittingParticipant: ParticipantReference,
      commands: CommandsWithMetadata,
  )(implicit traceContext: TraceContext): Future[TransactionTree] =
    waitForTransactionTree(submittingParticipant, commands, submitAsync_(commands))

  def waitForTransactionTree(
      submittingParticipant: ParticipantReference,
      commands: CommandsWithMetadata,
      doSubmit: => Future[Unit],
  ): Future[TransactionTree] = {
    val resultP = Promise[UpdateTreeWrapper]()

    val observer = new CollectFirstObserver[UpdateTreeWrapper]({
      case UpdateService.TransactionTreeWrapper(transactionTree) =>
        transactionTree.commandId == commands.commandId
      case _: UpdateService.ReassignmentWrapper => false
    })
    resultP.completeWith(observer.result)

    val closeSubscription = submittingParticipant.ledger_api.updates.subscribe_trees(
      observer,
      commands.filter,
      beginOffsetExclusive = submittingParticipant.ledger_api.state.end(),
      verbose = false,
    )
    resultP.future.onComplete(_ => closeSubscription.close())

    doSubmit.onComplete {
      case Success(_) => // nothing to do
      case Failure(exception) => resultP.tryFailure(exception)
    }

    resultP.future.map {
      case UpdateService.TransactionTreeWrapper(transactionTree) => transactionTree
      case other => sys.error(s"Unexpected update: $other")
    }
  }

  def submitAndWaitForCompletion(
      submittingParticipant: ParticipantReference,
      commands: CommandsWithMetadata,
  )(implicit traceContext: TraceContext): Future[Completion] =
    waitForCompletion(submittingParticipant, commands, submitAsync_(commands))

  def submitTransactionAndWaitForCompletion(
      submittingParticipant: ParticipantReference,
      commands: CommandsWithMetadata,
      transaction: SubmittedTransaction,
      nodeSeeds: ImmArray[(NodeId, crypto.Hash)],
      globalKeyInputs: Map[LfGlobalKey, Option[LfContractId]],
  )(implicit traceContext: TraceContext): Future[Completion] = {
    val submitterInfo = commands.submitterInfo(maxDeduplicationDuration.duration)
    val meta = commands.transactionMeta(transaction, nodeSeeds)
    waitForCompletion(
      submittingParticipant,
      commands,
      submitTransaction_(submitterInfo, meta, transaction, globalKeyInputs),
    )
  }

  def waitForCompletion(
      submittingParticipant: ParticipantReference,
      commands: CommandsWithMetadata,
      doSubmit: => Future[Unit],
  ): Future[Completion] = {
    val resultP = Promise[Completion]()

    val observer =
      new CollectFirstObserver[Completion](_.commandId == commands.commandId)
    resultP.completeWith(observer.result)

    val closeSubscription = submittingParticipant.ledger_api.completions.subscribe(
      observer,
      commands.parties,
    )
    resultP.future.onComplete(_ => closeSubscription.close())

    doSubmit.onComplete {
      case Success(_) => // nothing to do
      case Failure(exception) => resultP.tryFailure(exception)
    }

    resultP.future
  }

  class CollectFirstObserver[A](isAcceptable: A => Boolean) extends StreamObserver[A] {
    private val resultP: Promise[A] = Promise[A]()

    val result: Future[A] = resultP.future

    override def onNext(value: A): Unit = if (isAcceptable(value)) resultP.trySuccess(value)

    override def onError(t: Throwable): Unit = resultP.tryFailure(t)

    override def onCompleted(): Unit =
      resultP.tryFailure(new IllegalStateException("Subscription has terminated unexpectedly."))
  }

  def submitAsync_(
      commands: CommandsWithMetadata
  )(implicit traceContext: TraceContext): Future[Unit] =
    submitAsync(
      commands
    ).value.transform {
      case Success(Right(SubmissionResult.Acknowledged)) => TryUtil.unit
      case Success(Right(error: SubmissionResult.SynchronousError)) =>
        Failure(error.exception)
      case Success(Left(error)) =>
        Failure(new IllegalStateException(s"submitAsync has failed with an error: $error"))
      case Failure(exception) => Failure(exception)
    }

  def submitAsync(
      commands: CommandsWithMetadata
  )(implicit traceContext: TraceContext): EitherT[Future, Error, SubmissionResult] =
    for {
      transactionAndMetadata <- interpret(commands)
      (transaction, metadata) = transactionAndMetadata

      meta = commands.transactionMeta(transaction, metadata)

      submitterInfo = commands.submitterInfo(maxDeduplicationDuration.duration)

      result <- EitherT.right(
        submitTransaction(
          submitterInfo,
          meta,
          transaction,
          metadata.globalKeyMapping,
        )
      )
    } yield result

  def submitTransaction_(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      transaction: SubmittedTransaction,
      keyMapping: Map[LfGlobalKey, Option[LfContractId]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    submitTransaction(submitterInfo, meta, transaction, keyMapping).map {
      case SubmissionResult.Acknowledged => ()
      case error: SubmissionResult.SynchronousError => throw error.exception
    }

  def submitTransaction(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      transaction: SubmittedTransaction,
      keyMapping: Map[LfGlobalKey, Option[LfContractId]],
  )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
    val routingSynchronizerState = syncService.getRoutingSynchronizerState
    for {
      synchronizerRank <- syncService
        .selectRoutingSynchronizer(
          submitterInfo = submitterInfo,
          transaction = transaction,
          transactionMeta = meta,
          disclosedContractIds = List.empty,
          optSynchronizerId = None,
          transactionUsedForExternalSigning = false,
          routingSynchronizerState = routingSynchronizerState,
        )
        .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
        .merge
        .failOnShutdownToAbortException("test submit transaction")
      submissionResult <- syncService
        .submitTransaction(
          synchronizerRank = synchronizerRank,
          routingSynchronizerState = routingSynchronizerState,
          submitterInfo = submitterInfo,
          transactionMeta = meta,
          transaction = transaction,
          _estimatedInterpretationCost = 0,
          keyResolver = keyMapping,
          processedDisclosedContracts = ImmArray.Empty, // TODO(#9795) wire proper value
        )
        .toScalaUnwrapped
    } yield submissionResult
  }

  def interpret(commands: CommandsWithMetadata)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, (SubmittedTransaction, Transaction.Metadata)] =
    interpret(
      actAs = commands.actAs,
      apiCommands = commands.apiCommands(),
      readAs = commands.readAs,
      disclosures = commands.disclosures,
      submissionSeed = commands.submissionSeed,
      packagePreferenceOverride = commands.packagePreferenceOverride,
      packageMapOverride = commands.packageMapOverride,
    )

  def interpret(
      actAs: Seq[PartyId],
      apiCommands: ApiCommands,
      readAs: Seq[PartyId],
      disclosures: ImmArray[FatContractInstance] = ImmArray.Empty,
      submissionSeed: crypto.Hash = WeakRandom.nextSeed(),
      @unused packageMapOverride: Option[
        Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
      ] = None,
      @unused packagePreferenceOverride: Option[Set[Ref.PackageId]] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, (SubmittedTransaction, Transaction.Metadata)] = {
    val result =
      damle.submit(
        packageMap = packageMapOverride.getOrElse(basePackageMap),
        packagePreference = packagePreferenceOverride.getOrElse(basePackageMap.keySet),
        submitters = actAs.map(_.toLf).toSet,
        readAs = readAs.map(_.toLf).toSet,
        cmds = apiCommands,
        disclosures = disclosures,
        participantId = participantId.toLf,
        prefetchKeys = Seq.empty,
        submissionSeed = submissionSeed,
      )

    EitherT(resolve(result))
  }

  private def resolve(
      result: Result[(SubmittedTransaction, Transaction.Metadata)]
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Error, (SubmittedTransaction, Transaction.Metadata)]] = {
    @tailrec
    def iterateOverInterrupts[A](continue: () => Result[A]): Result[A] =
      continue() match {
        case ResultInterruption(continue, _) => iterateOverInterrupts(continue)
        case otherResult => otherResult
      }

    result match {
      case ResultDone(result) => Future.successful(Right(result))

      case ResultError(err) => Future.successful(Left(err))

      case ResultNeedContract(acoid, resume) =>
        for {
          fatContractO <- contractResolver(acoid)(traceContext)
          r <- resolve(resume(fatContractO))
        } yield r

      case ResultNeedPackage(packageId, resume) =>
        for {
          pckgO <- packageResolver(packageId)(traceContext).failOnShutdownToAbortException(
            "TestSubmissionService"
          )
          r <- resolve(resume(pckgO))
        } yield r

      case ResultNeedKey(key, resume) =>
        val gk = key.globalKey
        for {
          cidO <- keyResolver.resolveKey(gk)(traceContext)
          r <- resolve(resume(cidO))
        } yield r

      case ResultInterruption(continue, _) =>
        resolve(iterateOverInterrupts(continue))

      case ResultNeedUpgradeVerification(_, _, _, _, resume) =>
        resolve(resume(None))

      case ResultPrefetch(_, _, resume) => resolve(resume())
    }
  }
}

object TestSubmissionService {

  /** Creates a `TestSubmissionService` for submitting commands to the given `participant`. The
    * service invokes DAMLe to produce a transaction and directly submits the transaction to the
    * SyncService of `participant`. Unlike the ledger api server:
    *   - The transaction may use archived contracts.
    *   - The transaction may use contracts divulged to parties other than the parties specified in
    *     `actAs` or `readAs`.
    *   - A key may get resolved to a divulged contract.
    *   - With a `customKeyResolver`, key resolution can be customized further.
    *
    * These differences to the ledger api server have been introduced on purpose. They enable
    * testing the Canton conflict detection with transaction that would otherwise have been rejected
    * by the ledger api server.
    *
    * Note that resolution of contracts and keys is quite inefficient at the moment, but it can be
    * optimized, if need be.
    *
    * @param checkAuthorization
    *   whether to do authorization checks when running DAMLe.
    */
  def apply(
      participant: LocalParticipantReference,
      customKeyResolver: Option[TestKeyResolver] = None,
      checkAuthorization: Boolean = true,
      enableLfDev: Boolean = false,
      companionPackages: Seq[ContractTypeCompanion.Package] = Seq.empty,
  )(implicit env: TestConsoleEnvironment): TestSubmissionService = {
    import env.*

    val participantNode = participant.underlying.value

    val damle = new Engine(
      EngineConfig(
        allowedLanguageVersions =
          if (enableLfDev)
            LanguageVersion.AllVersions(LanguageVersion.Major.V2)
          else
            LanguageVersion.StableVersions(LanguageVersion.Major.V2),
        checkAuthorization = checkAuthorization,
      )
    )

    def resolveContract(
        coid: LfContractId
    )(traceContext: TraceContext): Future[Option[FatContractInstance]] =
      participantNode.sync.participantNodePersistentState.value.contractStore
        .lookupFatContract(coid)(traceContext)
        .value
        .failOnShutdownToAbortException("TestSubmissionService")

    val keyResolver = customKeyResolver.getOrElse(ActiveKeyResolver(participant))

    val packageResolver: PackageResolver = id =>
      tc => participantNode.sync.packageService.value.getPackage(id)(tc)

    val loggerFactory = participantNode.loggerFactory

    val packageMap = buildPackageMap(companionPackages)

    new TestSubmissionService(
      participant.id,
      participantNode.sync.maxDeduplicationDuration,
      damle,
      resolveContract,
      keyResolver,
      packageResolver,
      participantNode.sync,
      packageMap,
      loggerFactory,
    )
  }

  def buildPackageMap(
      pkgs: Seq[ContractTypeCompanion.Package]
  ): Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] =
    pkgs.view.map { p =>
      val id = Ref.PackageId.assertFromString(p.id)
      val name = Ref.PackageName.assertFromString(p.name)
      val version = Ref.PackageVersion.assertFromString(p.version.toString)
      id -> (name, version)
    }.toMap

  /** Strategy to resolve a contract key for use by DAML engine.
    */
  trait TestKeyResolver {
    def resolveKey(key: LfGlobalKey)(implicit
        traceContext: TraceContext
    ): Future[Option[LfContractId]]
  }

  /** Resolves a key to the first that matches:
    *   - the id of an active contract, if any,
    *   - the id of an archived contract, if any and `resolveToActive == false`,
    *   - `None`
    */
  abstract class StateBasedKeyResolver(
      participant: LocalParticipantReference,
      resolveToActive: Boolean,
  )(implicit
      executionContext: ExecutionContext
  ) extends TestKeyResolver {

    override def resolveKey(
        globalKey: LfGlobalKey
    )(implicit traceContext: TraceContext): Future[Option[LfContractId]] = {
      val syncService = participant.underlying.value.sync
      val contractStore = syncService.syncPersistentStateManager.contractStore.value

      for {
        idsBySynchronizer <-
          syncService.syncPersistentStateManager.getAll.toList
            .parTraverse { case (_, state) =>
              for {
                allContracts <- contractStore
                  .find(None, None, None, Int.MaxValue)
                  .failOnShutdownToAbortException("resolveKey")
                contractsWithKey = allContracts.filter(_.metadata.maybeKey.contains(globalKey))
                allContractIds = contractsWithKey.map(_.contractId)
                // At this stage, the ledger api server would filter out divulged contracts that are invisible to all submitters.
                activeContractIds <- contractsWithKey.parTraverseFilter { contract =>
                  for {
                    contractState <- state.activeContractStore
                      .fetchStates(Seq(contract.contractId))(traceContext)
                      .map(_.get(contract.contractId))
                      .failOnShutdownToAbortException("resolveKey")
                  } yield contractState.filter(_.status.isActive).map(_ => contract.contractId)
                }
              } yield (activeContractIds, allContractIds)
            }
      } yield {
        val (activeContractIds, allContractIds) = idsBySynchronizer.separate
        activeContractIds.flatten.headOption match {
          case Some(activeContractId) => Some(activeContractId)
          case None if resolveToActive => None
          case None => allContractIds.flatten.headOption
        }
      }
    }
  }

  /** Resolves a key to the id of an active contract with that key, if any, and to `None`,
    * otherwise.
    */
  final case class ActiveKeyResolver(
      participant: LocalParticipantReference
  )(implicit
      executionContext: ExecutionContext
  ) extends StateBasedKeyResolver(participant, resolveToActive = true)

  /** Resolves a key to the first that matches:
    *   - the id of an active contract, if any,
    *   - the id of an archived contract, if any,
    *   - `None`
    */
  final case class LastAssignedKeyResolver(
      participant: LocalParticipantReference
  )(implicit
      executionContext: ExecutionContext
  ) extends StateBasedKeyResolver(participant, resolveToActive = false)

  /** Resolves every key to `None`. */
  case object EmptyKeyResolver extends TestKeyResolver {
    override def resolveKey(key: LfGlobalKey)(implicit
        traceContext: TraceContext
    ): Future[None.type] =
      Future.successful(None)
  }

  final case class CommandsWithMetadata(
      commands: Seq[Command],
      actAs: Seq[PartyId],
      readAs: Seq[PartyId] = Seq.empty,
      commandId: String = UUID.randomUUID().toString,
      workflowIdStr: String = "",
      submissionId: String = UUID.randomUUID().toString,
      deduplicationPeriodO: Option[DeduplicationPeriod] = None,
      ledgerTime: Time.Timestamp = Time.Timestamp.now(),
      disclosures: ImmArray[FatContractInstance] = ImmArray.Empty,
      submissionSeed: crypto.Hash = WeakRandom.nextSeed(),
      packageMapOverride: Option[Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]] = None,
      packagePreferenceOverride: Option[Set[Ref.PackageId]] = None,
  ) {

    def parties: Seq[PartyId] = (actAs ++ readAs).distinct

    def apiCommands()(implicit errorLogger: ErrorLoggingContext): ApiCommands = {
      val apiCommands = new CommandsValidator(
        validateUpgradingPackageResolutions = ValidateUpgradingPackageResolutions.Empty,
        validateDisclosedContracts = ValidateDisclosedContracts.WithContractIdVerificationDisabled,
      )
        .validateInnerCommands(commands)
        .valueOr(throw _)
        .to(ImmArray)

      ApiCommands(apiCommands, ledgerTime, workflowIdStr)
    }

    def workflowIdO: Option[WorkflowId] = WorkflowId.fromString(workflowIdStr).toOption

    def submitterInfo(maxDeduplicationDuration: Duration): SubmitterInfo = {
      val deduplicationPeriod = deduplicationPeriodO.getOrElse(
        DeduplicationPeriod.DeduplicationDuration(maxDeduplicationDuration)
      )
      SubmitterInfo(
        actAs.toList.map(_.toLf),
        readAs.toList.map(_.toLf),
        // Hard-coding our default user id to make sure that submissions and subscriptions use the same value
        UserId.assertFromString(LedgerApiCommands.defaultUserId),
        CommandId.assertFromString(commandId),
        deduplicationPeriod,
        SubmissionId.fromString(submissionId).toOption,
        externallySignedSubmission = None,
      )
    }

    def transactionMeta(
        transaction: SubmittedTransaction,
        metadata: Transaction.Metadata,
    ): TransactionMeta =
      transactionMeta(
        transaction,
        metadata.nodeSeeds,
        metadata.submissionTime,
        metadata.usedPackages,
      )

    def transactionMeta(
        transaction: SubmittedTransaction,
        nodeSeeds: ImmArray[(NodeId, crypto.Hash)],
        submissionTime: Time.Timestamp = ledgerTime,
        usedPackages: Set[LfPackageId] = Set.empty,
    ): TransactionMeta = {
      val byKeyNodes = transaction.nodes
        .collect { case (nodeId, node: Node.Action) if node.byKey => nodeId }
        .to(ImmArray)

      TransactionMeta(
        ledgerTime,
        workflowIdO,
        submissionTime,
        submissionSeed,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        Some(usedPackages),
        Some(nodeSeeds),
        Some(byKeyNodes),
      )
    }

    def filter: TransactionFilter = TransactionFilter(
      filtersByParty = (actAs ++ readAs)
        .map(party =>
          party.toLf -> Filters(
            Seq(
              CumulativeFilter.defaultInstance
                .withWildcardFilter(WildcardFilter(includeCreatedEventBlob = false))
            )
          )
        )
        .toMap,
      filtersForAnyParty = None,
    )
  }
}
