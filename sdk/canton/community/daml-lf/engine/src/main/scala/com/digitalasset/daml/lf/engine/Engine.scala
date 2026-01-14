// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, ParticipantId, Party, TypeConId}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.{Error => IError}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language._
import com.digitalasset.daml.lf.speedy.metrics.MetricPlugin
import com.digitalasset.daml.lf.speedy.Question.Update
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBFetchTemplate
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SEMakeClo, SEValue, SExpr}
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue.SContractId
import com.digitalasset.daml.lf.speedy.Speedy.{Machine, PureMachine, UpdateMachine}
import com.digitalasset.daml.lf.speedy._
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  Node,
  SubmittedTransaction,
  TransactionCoder,
  Versioned,
  VersionedTransaction,
  Transaction => Tx,
}

import java.nio.file.{Files, StandardOpenOption}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value, ValueCoder}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, LookupError, PackageInterface}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.newTraceLog
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.validation.Validation
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._

// TODO once the ContextualizedLogger is replaced with the NamedLogger and Speedy doesn't use its
//   own logger, we can remove this import
trait EngineLogger {
  def add(message: String)(implicit loggingContext: LoggingContext): Unit
}

object EngineLogger {
  def toTraceLog(logger: Option[EngineLogger]): TraceLog = logger match {
    case Some(value) =>
      new TraceLog {
        override def add(
            message: String,
            optLocation: Option[com.digitalasset.daml.lf.data.Ref.Location],
        )(implicit
            loggingContext: LoggingContext
        ): Unit = value.add(message)
        override def iterator
            : Iterator[(String, Option[com.digitalasset.daml.lf.data.Ref.Location])] =
          Iterator.empty
      }
    case None => newTraceLog
  }
}

/** Allows for evaluating [[Commands]] and validating [[Transaction]]s.
  * <p>
  *
  * This class does not dereference contract ids or package ids on its own.
  * Instead, when an instance of this class needs to dereference a contract id or package id,
  * it returns a [[ResultNeedContract]] or [[ResultNeedPackage]] to the caller.
  * The caller can then resume the computation by calling `result.resume`.
  * The engine may or may not cache and reuse the provided contract instance or package.
  * <p>
  *
  * The caller must dereference contract and package ids consistently, i.e.,
  * if the '''same engine''' returns `result1` and `result2`,
  * `result1` and `result2` request to dereference the same contract or package id, and
  * the caller invokes `result1.resume(x1)` and `result2.resume(x2)`,
  * then `x1` must equal `x2`.
  * <p>
  *
  * The caller may deference ids inconsistently across different engines.
  * Namely, if '''two different engines''' return `result1` and `result2`,
  * then the caller may call `result1.resume(x1)` and `result2.resume(x2)` with `x1 != x2`,
  * even if `result1` and `result2` request to dereference the same id.
  * <p>
  *
  * The class requires a pseudo random generator (`nextRandomInt`) to randomize the
  * preparation time. This generator does not have to be cryptographically secure.
  * <p>
  *
  * This class is thread safe as long `nextRandomInt` is.
  */
class Engine(val config: EngineConfig) {

  config.profileDir.foreach(Files.createDirectories(_))
  config.snapshotDir.foreach(Files.createDirectories(_))

  private[this] val compiledPackages = ConcurrentCompiledPackages(config.getCompilerConfig)

  private[this] val stablePackageIds = StablePackages.ids(config.allowedLanguageVersions)

  private[engine] val preprocessor =
    new preprocessing.Preprocessor(
      compiledPackages = compiledPackages,
      loadPackage = loadPackage,
      forbidLocalContractIds = config.forbidLocalContractIds,
      costModel = data.CostModel.EmptyCostModelImplicits,
    )

  def info = new EngineInfo(config)

  /** Interprets a sequence of commands `cmds` to the corresponding `SubmittedTransaction` and `Tx.Metadata`.
    * Requests data required during the interpretation (such as the contract or package corresponding to a given id)
    * through the `Result` subclasses.
    *
    * The resulting transaction (if any) meets the following properties:
    * <ul>
    *   <li>The transaction is well-typed and conforms to the DAML model described by the packages supplied via `ResultNeedPackage`.
    *       In particular, each contract created by the transaction meets the ensures clauses of the underlying template.</li>
    *   <li>The transaction paired with `submitters` is well-authorized according to the ledger model.</li>
    *   <li>The transaction is annotated with the packages used during interpretation.</li>
    * </ul>
    *
    * @param packageMap all the package known by the ledger with their name and version
    * @param packagePreference the set of package that should be use to resolve package name in command and interface exercise
    *                          packageReference should not contain two package with the same name
    * @param submitters the parties authorizing the root actions (both read and write) of the resulting transaction
    *                   ("committers" according to the ledger model)
    * @param readAs the parties authorizing the root actions (only read, but no write) of the resulting transaction
    * @param cmds the commands to be interpreted
    * @param participantId a unique identifier (of the underlying participant) used to derive node and contractId discriminators
    * @param submissionSeed the master hash used to derive node and contractId discriminators
    * @param contractIdVersion The contract ID version to use for local contracts
    */
  def submit(
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      packagePreference: Set[Ref.PackageId] = Set.empty,
      submitters: Set[Party],
      readAs: Set[Party],
      cmds: ApiCommands,
      participantId: ParticipantId,
      submissionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      prefetchKeys: Seq[ApiContractKey],
      engineLogger: Option[EngineLogger] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    val preparationTime = cmds.ledgerEffectiveTime

    for {
      pkgResolution <- preprocessor.buildPackageResolution(packageMap, packagePreference)
      processedCmds <- preprocessor.preprocessApiCommands(pkgResolution, cmds.commands)
      processedPrefetchKeys <- preprocessor.preprocessApiContractKeys(pkgResolution, prefetchKeys)
      _ <- preprocessor.prefetchContractIdsAndKeys(
        processedCmds,
        processedPrefetchKeys,
        unprocessedCommands = Some(cmds.commands),
      )
      // TODO: https://github.com/digital-asset/daml/issues/21933: Preprocessing input size checks should stop submission workflows ASAP
      _ <- preprocessor.getInputCost
      result <-
        interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = readAs,
          commands = processedCmds,
          ledgerTime = cmds.ledgerEffectiveTime,
          preparationTime = preparationTime,
          seeding = Engine.initialSeeding(submissionSeed, participantId, preparationTime),
          contractIdVersion = contractIdVersion,
          packageResolution = pkgResolution,
          engineLogger = engineLogger,
          submissionInfo = Some(Engine.SubmissionInfo(participantId, submissionSeed, submitters)),
        )
      (tx, meta, _) = result
    } yield (tx, meta)
  }

  /** Behaves like `submit`, but it takes a single `ReplayCommand` (instead of `ApiCommands`) as input.
    * It can be used to reinterpret partially an already interpreted transaction.
    *
    * If the command would fail with an unhandled exception, we return a transaction containing a
    * single rollback node. (This is achieving by compiling with `unsafeCompileForReinterpretation`
    * which wraps the command with a catch-everything exception handler.)
    *
    * @param nodeSeed the seed of the root node as generated during submission.
    *                 If undefined the contract IDs are derive using V0 scheme.
    *                 The value does not matter for other kind of nodes.
    * @param preparationTime the preparation time used to compute contract IDs
    * @param ledgerEffectiveTime the ledger effective time used as a result of `getTime` during reinterpretation
    * @param contractIdVersion The contract ID version to use for local contracts
    */
  def reinterpret(
      submitters: Set[Party],
      command: ReplayCommand,
      nodeSeed: Option[crypto.Hash],
      preparationTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] =
    for {
      speedyCommand <- preprocessor.preprocessReplayCommand(command)
      sexpr <- runCompilerSafely(
        NameOf.qualifiedNameOfCurrentFunc,
        compiledPackages.compiler.unsafeCompileForReinterpretation(speedyCommand),
      )
      // reinterpret is never used for submission, only for validation.
      result <- interpretExpression(
        validating = true,
        submitters = submitters,
        readAs = Set.empty,
        sexpr = sexpr,
        ledgerTime = ledgerEffectiveTime,
        preparationTime = preparationTime,
        seeding = InitialSeeding.RootNodeSeeds(ImmArray(nodeSeed)),
        contractIdVersion: ContractIdVersion,
        packageResolution = packageResolution,
        engineLogger = engineLogger,
        submissionInfo = None,
      )
      (tx, meta, _) = result
    } yield (tx, meta)

  def replay(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] =
    replayAndCollectMetrics(
      submitters,
      tx,
      ledgerEffectiveTime,
      participantId,
      preparationTime,
      submissionSeed,
      contractIdVersion,
      packageResolution,
      engineLogger,
    ).map { case (tx, meta, _) => (tx, meta) }

  private[lf] def replayAndCollectMetrics(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  )(implicit
      loggingContext: LoggingContext
  ): Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] =
    for {
      commands <- preprocessor.translateTransactionRoots(tx)
      result <- interpretCommands(
        validating = true,
        submitters = submitters,
        readAs = Set.empty,
        commands = commands,
        ledgerTime = ledgerEffectiveTime,
        preparationTime = preparationTime,
        seeding = Engine.initialSeeding(submissionSeed, participantId, preparationTime),
        contractIdVersion = contractIdVersion,
        packageResolution = packageResolution,
        engineLogger = engineLogger,
        submissionInfo = None,
        metricPlugins = metricPlugins,
      )
    } yield result

  /** Check if the given transaction is a valid result of some single-submitter command.
    *
    * Formally, for all tx, pcs, pkgs, keys:
    *   evaluate(validate(tx, ledgerEffectiveTime)) == ResultDone(()) <==> exists cmds. evaluate(submit(cmds)) = tx
    * where:
    *   evaluate(result) = result.consume(pcs, pkgs, keys)
    *
    * A transaction may contain relative contract IDs and still pass validation, but not in the root nodes.
    *
    * This is enforced since commands cannot contain relative contract ids, and we check that root nodes come from commands.
    *
    *  @param tx a complete unblinded Transaction to be validated
    *  @param ledgerEffectiveTime time when the transaction is claimed to be submitted
    */
  def validate(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  )(implicit loggingContext: LoggingContext): Result[Unit] = {
    validateAndCollectMetrics(
      submitters,
      tx,
      ledgerEffectiveTime,
      participantId,
      preparationTime,
      submissionSeed,
      contractIdVersion,
      metricPlugins,
    ).map(_ => ())
  }

  private[lf] def validateAndCollectMetrics(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  )(implicit loggingContext: LoggingContext): Result[Speedy.Metrics] = {
    // reinterpret
    for {
      result <- replayAndCollectMetrics(
        submitters,
        tx,
        ledgerEffectiveTime,
        participantId,
        preparationTime,
        submissionSeed,
        contractIdVersion,
        metricPlugins = metricPlugins,
      )
      (rtx, _, metrics) = result
      validationResult <-
        transaction.Validation
          .isReplayedBy(tx, rtx)
          .fold(
            e => ResultError(Error.Validation.ReplayMismatch(e)),
            _ => ResultDone(metrics),
          )
    } yield validationResult
  }

  /** Builds a GlobalKey based on a normalised representation of the provided key
    * @param templateId - the templateId associated with the contract key
    * @param key - a representation of the key that may be un-normalized
    */
  def buildGlobalKey(templateId: Identifier, key: Value): Result[GlobalKey] = {
    preprocessor.buildGlobalKey(
      templateId: Identifier,
      key: Value,
      extendLocalIdForbiddanceToRelativeV2 = false,
    )
  }

  private[engine] def loadPackage(pkgId: PackageId, context: language.Reference): Result[Unit] =
    ResultNeedPackage(
      pkgId,
      {
        case Some(pkg) =>
          compiledPackages.addPackage(pkgId, pkg)
        case None =>
          ResultError(Error.Package.MissingPackage(pkgId, context))
      },
    )

  @inline
  private[this] def runCompilerSafely[X](funcName: => String, run: => X): Result[X] =
    try {
      ResultDone(run)
    } catch {
      // The two following error should be prevented by the type checking does by translateCommand
      // so it’s an internal error.
      case speedy.Compiler.PackageNotFound(pkgId, context) =>
        ResultError(
          Error.Preprocessing.Internal(
            funcName,
            s"CompilationError: " + LookupError.MissingPackage.pretty(pkgId, context),
            None,
          )
        )
      case err @ speedy.Compiler.CompilationError(msg) =>
        ResultError(Error.Preprocessing.Internal(funcName, s"CompilationError: $msg", Some(err)))
    }

  // command-list compilation, followed by interpretation
  private[engine] def interpretCommands(
      validating: Boolean,
      submitters: Set[Party],
      readAs: Set[Party],
      commands: ImmArray[speedy.Command],
      ledgerTime: Time.Timestamp,
      preparationTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  )(implicit
      loggingContext: LoggingContext
  ): Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] =
    for {
      sexpr <- runCompilerSafely(
        NameOf.qualifiedNameOfCurrentFunc,
        compiledPackages.compiler.unsafeCompile(commands),
      )
      result <- interpretExpression(
        validating,
        submitters,
        readAs,
        sexpr,
        ledgerTime,
        preparationTime,
        seeding,
        contractIdVersion,
        packageResolution,
        engineLogger,
        submissionInfo,
        metricPlugins,
      )
    } yield result

  /** Interprets the given commands under the authority of @submitters, with additional readers @readAs
    *
    * Submitters are a set, in order to support interpreting subtransactions
    * (a subtransaction can be authorized by multiple parties).
    *
    * [[seeding]] is seeding used to derive node seed and contractId discriminator.
    */
  private[engine] def interpretExpression(
      validating: Boolean,
      /* See documentation for `Speedy.Machine` for the meaning of this field */
      submitters: Set[Party],
      readAs: Set[Party],
      sexpr: SExpr,
      ledgerTime: Time.Timestamp,
      preparationTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      engineLogger: Option[EngineLogger] = None,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  )(implicit
      loggingContext: LoggingContext
  ): Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] = {

    val machine = UpdateMachine(
      compiledPackages = compiledPackages,
      preparationTime = preparationTime,
      initialSeeding = seeding,
      expr = SEApp(sexpr, ArraySeq(SValue.SToken)),
      committers = submitters,
      readAs = readAs,
      authorizationChecker = config.authorizationChecker,
      validating = validating,
      traceLog = EngineLogger.toTraceLog(engineLogger),
      contractKeyUniqueness = config.contractKeyUniqueness,
      contractIdVersion = contractIdVersion,
      packageResolution = packageResolution,
      limits = config.limits,
      iterationsBetweenInterruptions = config.iterationsBetweenInterruptions,
      initialGasBudget = config.gasBudget,
      metricPlugins = metricPlugins,
    )
    interpretLoop(machine, ledgerTime, submissionInfo)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  @scala.annotation.nowarn("msg=return statement uses an exception to pass control to the caller")
  private[engine] def deps(tx: VersionedTransaction): Result[Set[PackageId]] = {
    val nodePkgIds =
      tx.nodes.values.collect { case node: Node.Action => node.packageIds }.flatten.toSet
    val deps = nodePkgIds.foldLeft(nodePkgIds)((acc, pkgId) =>
      acc | compiledPackages
        .getPackageDependencies(pkgId)
        .getOrElse(
          return ResultError(
            Error.Interpretation.Internal(
              NameOf.qualifiedNameOfCurrentFunc,
              s"INTERNAL ERROR: Missing dependencies of package $pkgId",
              None,
            )
          )
        )
    )
    ResultDone(deps)
  }

  private def handleError(err: SError.SError, detailMsg: Option[String]): ResultError = {
    err match {
      case SError.SErrorDamlException(error) =>
        ResultError(Error.Interpretation.DamlException(error), detailMsg)
      case err @ SError.SErrorCrash(where, reason) =>
        ResultError(Error.Interpretation.Internal(where, reason, Some(err)))
    }
  }

  private lazy val enricher = new Enricher(
    compiledPackages,
    loadPackage,
    addTypeInfo = true,
    addFieldNames = true,
    addTrailingNoneFields = true,
    forbidLocalContractIds = false,
  )

  private[engine] def interpretLoop(
      machine: UpdateMachine,
      time: Time.Timestamp,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
  ): Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] = {
    val abort = () => {
      machine.abort()
      Some(machine.transactionTrace(config.transactionTraceMaxLength))
    }

    def finish: Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] =
      machine.finish match {
        case Right(
              UpdateMachine.Result(tx, _, nodeSeeds, globalKeyMapping)
            ) =>
          deps(tx).flatMap { deps =>
            if (config.paranoid) {
              (for {
                // check the transaction can be serialized and deserialized
                encoded <- TransactionCoder
                  .encodeTransaction(tx)
                  .left
                  .map("transaction encoding fails: " + _)
                decoded <- TransactionCoder
                  .decodeTransaction(encoded)
                  .left
                  .map("transaction decoding fails: " + _)
                _ <- Either.cond(
                  tx == decoded,
                  (),
                  "transaction encoding/decoding is not idempotent",
                )
                // check that impoverishment is indempotent on engine output
                poor = Enricher.impoverish(tx)
                _ <- Either.cond(
                  tx == poor,
                  (),
                  "transaction impoverishment is not idempotent on engine output",
                )
                // check that impoverishment remove the data added by enrichement
                rich <- enricher
                  .enrichVersionedTransaction(tx)
                  .consume()
                  .left
                  .map("transaction enrichment fails: " + _)
                poor = Enricher.impoverish(rich)
                _ <- Either.cond(
                  tx == poor,
                  (),
                  "transaction enrichment/impoverishment is not idempotent",
                )
              } yield ()).fold(err => throw new java.lang.AssertionError(err), identity)
            }

            val meta = Tx.Metadata(
              submissionSeed = submissionInfo.map(_.submissionSeed),
              preparationTime = machine.preparationTime,
              usedPackages = deps,
              timeBoundaries = machine.getTimeBoundaries,
              nodeSeeds = nodeSeeds,
              globalKeyMapping = globalKeyMapping,
            )

            config.profileDir.foreach { dir =>
              val desc = Engine.profileDesc(tx)
              val profileFile = dir.resolve(s"${meta.preparationTime}-$desc.json")
              machine.profile.name = s"${meta.preparationTime}-$desc"
              machine.profile.writeSpeedscopeJson(profileFile)
            }
            val snapshotResult = config.snapshotDir.zip(submissionInfo).flatMap {
              case (dir, Engine.SubmissionInfo(participantId, submissionSeed, submitters)) =>
                // Ensure OSes don't complain about filename characters - e.g. ':'
                val snapshotFile =
                  dir.resolve(s"snapshot-${participantId.replaceAll("[^a-zA-Z0-9-_\\.]", "_")}.bin")
                TransactionCoder
                  .encodeTransaction(tx)
                  .fold(
                    err => Some(("TransactionCoder.encodeTransaction", err)),
                    encoded => {
                      val txEntry = Snapshot.TransactionEntry
                        .newBuilder()
                        .setRawTransaction(encoded.toByteString)
                        .setParticipantId(participantId)
                        .addAllSubmitters(submitters.map(_.toString).asJava)
                        .setLedgerTime(time.micros)
                        .setPreparationTime(meta.preparationTime.micros)
                        .setSubmissionSeed(submissionSeed.bytes.toByteString)
                        .build()
                      val txSubmission = Snapshot.SubmissionEntry
                        .newBuilder()
                        .setTransaction(txEntry)
                        .build()

                      txSubmission.writeDelimitedTo(
                        Files.newOutputStream(
                          snapshotFile,
                          StandardOpenOption.CREATE,
                          StandardOpenOption.APPEND,
                        )
                      )

                      None
                    },
                  )
            }

            snapshotResult match {
              case Some((loc, ValueCoder.EncodeError(errMsg))) =>
                ResultError(Error.Interpretation.Internal(loc, errMsg, None))

              case None =>
                ResultDone((tx, meta, machine.metrics))
            }
          }
        case Left(err) =>
          handleError(err, None)
      }

    @scala.annotation.tailrec
    def loop: Result[(SubmittedTransaction, Tx.Metadata, Speedy.Metrics)] = {
      machine.run() match {

        case SResultQuestion(question) =>
          question match {

            case Question.Update.NeedTime(callback) =>
              callback(time)
              loop

            case Question.Update.NeedPackage(pkgId, context, callback) =>
              Result.needPackage(
                pkgId,
                context,
                { pkg: Package =>
                  compiledPackages.addPackage(pkgId, pkg).flatMap { _ =>
                    callback(compiledPackages)
                    interpretLoop(machine, time, submissionInfo)
                  }
                },
              )

            case Question.Update.NeedContract(coid, _, callback) =>
              Result.needContract(
                coid,
                { (coinst, hashMethod, authenticator) =>
                  callback(coinst, hashMethod, authenticator)
                  interpretLoop(machine, time, submissionInfo)
                },
              )

            case Question.Update.NeedKey(gk, _, callback) =>
              ResultNeedKey(
                gk,
                { coid: Option[ContractId] =>
                  discard[Boolean](callback(coid))
                  interpretLoop(machine, time, submissionInfo)
                },
              )
          }

        case SResultInterruption =>
          ResultInterruption(() => interpretLoop(machine, time, submissionInfo), abort)

        case _: SResultFinal =>
          finish

        case SResultError(err) =>
          handleError(err, Some(machine.transactionTrace(config.transactionTraceMaxLength)))
      }
    }

    loop
  }

  def clearPackages(): Unit = compiledPackages.clear()

  /** Note: it's important we return a [[com.digitalasset.daml.lf.CompiledPackages]],
    * and not a [[ConcurrentCompiledPackages]], otherwise people would be able
    * to modify them.
    */
  def compiledPackages(): CompiledPackages = compiledPackages

  /** This function can be used to give a package to the engine pre-emptively,
    * rather than having the engine to ask about it through
    * [[ResultNeedPackage]].
    *
    * Returns a [[Result]] because the package might need another package to
    * be loaded.
    */
  def preloadPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    compiledPackages.addPackage(pkgId, pkg)

  /** This method checks a Dar file is self-consistent (it
    * contains all its dependencies and only those dependencies), contains only well-formed
    * packages (See Daml-LF spec for more details) and uses only the
    * allowed language versions (as described by the engine
    * config).
    * This is not affected by [[config.packageValidation]] flag.
    * Package in [[pkgIds]] but not in [[pkgs]] are assumed to be
    * preloaded.
    */
  def validateDar(dar: Dar[(PackageId, Package)]): Either[Error.Package.Error, Unit] = {
    val pkgIdDepGraph: Map[PackageId, Set[PackageId]] =
      dar.all.toMap.view.mapValues(_.imports.pkgIds).toMap
    val mainPackageId: PackageId = dar.main._1

    val mentioned = Graphs.transitiveClosure(pkgIdDepGraph, mainPackageId).diff(stablePackageIds)
    val included = pkgIdDepGraph.keys.toSet.diff(stablePackageIds)
    val darPackages = dar.all.toMap

    for {
      // first do Version check, copied as-is from validateDar pre-imports-rework
      _ <- darPackages
        .collectFirst {
          case (pkgId, pkg)
              if !stablePackageIds.contains(pkgId) && !config.allowedLanguageVersions
                .contains(pkg.languageVersion) =>
            Error.Package.AllowedLanguageVersion(
              pkgId,
              pkg.languageVersion,
              config.allowedLanguageVersions,
            )
        }
        .toLeft(())

      // meat of the package checks, to check if included = mentioned
      _ <- Either.cond(
        included == mentioned,
        (),
        // we set transitiveDependencies to Set.empty because our logic does not generate it anymore
        Error.Package.DarSelfConsistency(
          mainPackageId = mainPackageId,
          missingDependencies = mentioned.diff(included),
          extraDependencies = included.diff(mentioned),
        ),
      )

      // rest of copied check from validateDar pre-imports-rework
      pkgInterface = PackageInterface(darPackages)
      _ <- {
        darPackages.iterator
          // we trust already loaded packages
          .collect {
            case (pkgId, pkg) if !compiledPackages.contains(pkgId) =>
              Validation.checkPackage(pkgInterface, pkgId, pkg)
          }
          .collectFirst { case Left(err) => Error.Package.Validation(err) }
      }.toLeft(())
    } yield ()
  }

  /** Given a contract argument of the given template id, calculate the interface
    * view of that API.
    */
  def computeInterfaceView(
      templateId: Identifier,
      argument: Value,
      interfaceId: Identifier,
  )(implicit loggingContext: LoggingContext): Result[Versioned[Value]] = {
    @scala.annotation.nowarn("msg=dead code following this construct")
    def interpret(machine: PureMachine, abort: () => Option[String]): Result[SValue] =
      machine.run() match {
        case SResultFinal(v) => ResultDone(v)
        case SResultError(err) => handleError(err, None)
        case SResult.SResultInterruption =>
          ResultInterruption(() => interpret(machine, abort), abort)
        case SResultQuestion(nothing) => nothing: Nothing
      }
    for {
      view <- preprocessor.preprocessInterfaceView(templateId, argument, interfaceId)
      sexpr <- runCompilerSafely(
        NameOf.qualifiedNameOfCurrentFunc,
        compiledPackages.compiler.unsafeCompileInterfaceView(view),
      )
      machine = Machine.fromPureSExpr(
        compiledPackages,
        sexpr,
        config.iterationsBetweenInterruptions,
      )
      r <- interpret(machine, () => { machine.abort(); None })
      version = machine.tmplId2TxVersion(interfaceId)
    } yield Versioned(version, r.toNormalizedValue)
  }

  /** Computes the hash of a Create node. Used for producing fat contract
    * instances after absolutizing the contract IDs of a transaction produced by
    * the engine.
    *
    * @param create the Create node for which the hash is computed
    * @param contractIdSubstitution a substitution for contract IDs to be applied by the engine to the create argument
    *                               before hashing
    * @param hashingMethod the hashing method to use
    * @return a Result containing the computed hash. When [hashingMethod] is
    *         [[HashingMethod.TypedNormalForm]], returns a [[ResultError]]
    *         if [[create]] is ill-typed or if its package is unavailable.
    */
  def hashCreateNode(
      create: Node.Create,
      contractIdSubstitution: ContractId => ContractId,
      hashingMethod: Hash.HashingMethod,
  ): Result[Hash] = {
    import Engine.Syntax._
    import Engine.mkInterpretationError

    val templateId = create.templateId
    val pkgId = templateId.packageId
    val createArg = create.arg.mapCid(contractIdSubstitution)
    val packageName = create.packageName

    def mkHashingError(msg: String): Error.Interpretation =
      mkInterpretationError(
        IError.ContractHashingError(create.coid, create.templateId, create.arg, msg)
      )

    hashingMethod match {
      case Hash.HashingMethod.Legacy =>
        Hash
          .hashContractInstance(templateId, createArg, packageName, upgradeFriendly = false)
          .left
          .map(mkHashingError)
          .toResult
      case Hash.HashingMethod.UpgradeFriendly =>
        Hash
          .hashContractInstance(templateId, createArg, packageName, upgradeFriendly = true)
          .left
          .map(mkHashingError)
          .toResult
      case Hash.HashingMethod.TypedNormalForm =>
        for {
          _ <-
            if (!compiledPackages.contains(pkgId))
              loadPackage(pkgId, language.Reference.Template(templateId.toRef))
            else ResultDone.Unit
          sValue <- new ValueTranslator(
            compiledPackages.pkgInterface,
            forbidLocalContractIds = true,
            forbidTrailingNones = true,
          )
            .translateValue(Ast.TTyCon(templateId), createArg)
            .left
            .map(error =>
              mkInterpretationError(
                IError.Upgrade(
                  IError.Upgrade.TranslationFailed(
                    Some(create.coid),
                    create.templateId,
                    create.templateId,
                    create.arg,
                    error,
                  )
                )
              )
            )
            .toResult
          hash <- SValueHash
            .hashContractInstance(packageName, templateId.qualifiedName, sValue)
            .left
            .map(error => mkHashingError(error.msg))
            .toResult
        } yield hash
    }
  }

  /** Validates [instance] against [targetPackageId] by performing the following checks:
    * - Verifies that the argument type checks against the target package
    * - Verifies that the ensures clause does not evaluate to false or throw
    * - Checks that the metadata in the contract is consistent with that produced by the target package
    * - Hashes the contract instance using the specified hashing method
    * - Validates this hash using the provided [idValidator]
    *
    * @param instance the contract instance to validate
    * @param targetPackageId the target package id against which the instance is validated
    * @param contractIdSubstitution a substitution for contract IDs to be applied by the engine to the contract
    *                                instance before validation
    * @param hashingMethod the hash type to use for validation
    * @param idValidator a function that checks whether a given hash is valid
    * @return a Result containing a [Right(())] on success and a [Left(_)] when validation fails. On other errors
    *         like missing packages or internal errors, a ResultError is returned.
    */
  def validateContractInstance(
      instance: FatContractInstance,
      targetPackageId: PackageId,
      contractIdSubstitution: ContractId => ContractId,
      hashingMethod: Hash.HashingMethod,
      idValidator: Hash => Boolean,
  )(implicit loggingContext: LoggingContext): Result[Either[IError, Unit]] = {
    val substitutedInstance = instance.mapCid(contractIdSubstitution)

    def internalError(msg: String): Result[Either[IError, Unit]] =
      ResultError(Error.Interpretation.Internal(NameOf.qualifiedNameOfCurrentFunc, msg, None))

    def interpret(
        machine: UpdateMachine,
        abort: () => Option[String],
    ): Result[Either[IError, Unit]] =
      machine.run() match {
        case SResult.SResultQuestion(question) =>
          question match {
            case Update.NeedContract(contractId, _, callback) =>
              if (contractId != substitutedInstance.contractId)
                internalError(
                  s"expected contract id ${substitutedInstance.contractId}, got $contractId"
                )
              else {
                discard(callback(substitutedInstance, hashingMethod, idValidator))
                interpret(machine, abort)
              }
            case Update.NeedPackage(pkgId, context, callback) =>
              Result.needPackage(
                pkgId,
                context,
                { pkg: Package =>
                  compiledPackages.addPackage(pkgId, pkg).flatMap { _ =>
                    callback(compiledPackages)
                    interpret(machine, abort)
                  }
                },
              )
            case _ => internalError(s"unexpected question from speedy: $question")
          }
        case SResult.SResultFinal(_) =>
          ResultDone(Right(()))
        case SResult.SResultError(err) =>
          err match {
            case SError.SErrorDamlException(error) =>
              ResultDone(Left(error))
            case err @ SError.SErrorCrash(where, reason) =>
              ResultError(Error.Interpretation.Internal(where, reason, Some(err)))
          }
        case SResult.SResultInterruption =>
          ResultInterruption(() => interpret(machine, abort), abort)
      }

    val machine =
      Speedy.Machine.fromUpdateSExpr(
        compiledPackages = compiledPackages,
        transactionSeed = crypto.Hash.hashPrivateKey("ContractValidationImpl.validate"),
        updateSE = SEMakeClo(
          ArraySeq.empty,
          1,
          SBFetchTemplate(TypeConId(targetPackageId, substitutedInstance.templateId.qualifiedName))(
            SEValue(SContractId(substitutedInstance.contractId))
          ),
        ),
        committers = Set.empty,
      )

    interpret(machine, () => { machine.abort(); None })
  }
}

object Engine {

  type Packages = Map[PackageId, Package]

  private[engine] final case class SubmissionInfo(
      participantId: Ref.ParticipantId,
      submissionSeed: crypto.Hash,
      submitters: Set[Ref.Party],
  )

  def initialSeeding(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, preparationTime)
    )

  private def profileDesc(tx: VersionedTransaction): String = {
    if (tx.roots.length == 1) {
      val makeDesc = (kind: String, tmpl: Ref.Identifier, extra: Option[String]) =>
        s"$kind:${tmpl.qualifiedName.name}${extra.map(extra => s":$extra").getOrElse("")}"
      tx.nodes.get(tx.roots(0)).toList.head match {
        case _: Node.Rollback => "rollback"
        case create: Node.Create => makeDesc("create", create.templateId, None)
        case exercise: Node.Exercise =>
          makeDesc("exercise", exercise.templateId, Some(exercise.choiceId))
        case fetch: Node.Fetch => makeDesc("fetch", fetch.templateId, None)
        case lookup: Node.LookupByKey => makeDesc("lookup", lookup.templateId, None)
      }
    } else {
      s"compound:${tx.roots.length}"
    }
  }

  def DevEngine: Engine = new Engine(
    EngineConfig(allowedLanguageVersions = LanguageVersion.allLfVersionsRange)
  )

  private def mkInterpretationError(error: IError) =
    Error.Interpretation(Error.Interpretation.DamlException(error), None)

  private object Syntax {
    implicit class EitherOps[A](val e: Either[Error, A]) extends AnyVal {
      def toResult: Result[A] = e.fold(ResultError(_), ResultDone(_))
    }
  }
}
