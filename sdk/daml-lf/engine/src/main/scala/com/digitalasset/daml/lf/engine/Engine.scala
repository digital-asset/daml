// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, ParticipantId, Party}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.{InitialSeeding, Question, SError, SResult, SValue, TraceLog}
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.Speedy.{Machine, PureMachine, UpdateMachine}
import com.digitalasset.daml.lf.speedy.SResult._
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
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.ValueCoder
import com.digitalasset.daml.lf.language.{
  LanguageMajorVersion,
  LanguageVersion,
  LookupError,
  PackageInterface,
}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.newTraceLog
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.validation.Validation
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec
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
      requireContractIdSuffix = config.requireSuffixedGlobalContractId,
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
    * @param disclosures contracts to be used as input contracts of the transaction;
    *                    contract data may come from an untrusted source and will therefore be validated during interpretation.
    * @param participantId a unique identifier (of the underlying participant) used to derive node and contractId discriminators
    * @param submissionSeed the master hash used to derive node and contractId discriminators
    */
  def submit(
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      packagePreference: Set[Ref.PackageId] = Set.empty,
      submitters: Set[Party],
      readAs: Set[Party],
      cmds: ApiCommands,
      disclosures: ImmArray[FatContractInstance] = ImmArray.empty,
      participantId: ParticipantId,
      submissionSeed: crypto.Hash,
      prefetchKeys: Seq[ApiContractKey],
      engineLogger: Option[EngineLogger] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    val preparationTime = cmds.ledgerEffectiveTime

    for {
      pkgResolution <- preprocessor.buildPackageResolution(packageMap, packagePreference)
      processedCmds <- preprocessor.preprocessApiCommands(pkgResolution, cmds.commands)
      processedPrefetchKeys <- preprocessor.preprocessApiContractKeys(pkgResolution, prefetchKeys)
      preprocessedDiscsAndKeys <- preprocessor.preprocessDisclosedContracts(disclosures)
      (processedDiscs, disclosedContractIds, disclosedKeyHashes) = preprocessedDiscsAndKeys
      _ <- preprocessor.prefetchContractIdsAndKeys(
        processedCmds,
        processedPrefetchKeys,
        disclosedContractIds,
        disclosedKeyHashes,
      )
      result <-
        interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = readAs,
          commands = processedCmds,
          disclosures = processedDiscs,
          ledgerTime = cmds.ledgerEffectiveTime,
          preparationTime = preparationTime,
          seeding = Engine.initialSeeding(submissionSeed, participantId, preparationTime),
          packageResolution = pkgResolution,
          engineLogger = engineLogger,
          submissionInfo = Some(Engine.SubmissionInfo(participantId, submissionSeed, submitters)),
        )
    } yield result
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
    */
  def reinterpret(
      submitters: Set[Party],
      command: ReplayCommand,
      nodeSeed: Option[crypto.Hash],
      preparationTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
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
        packageResolution = packageResolution,
        engineLogger = engineLogger,
        submissionInfo = None,
      )
    } yield result

  def replay(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      preparationTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] =
    for {
      commands <- preprocessor.translateTransactionRoots(tx)
      result <- interpretCommands(
        validating = true,
        submitters = submitters,
        readAs = Set.empty,
        commands = commands,
        disclosures = ImmArray.empty,
        ledgerTime = ledgerEffectiveTime,
        preparationTime = preparationTime,
        seeding = Engine.initialSeeding(submissionSeed, participantId, preparationTime),
        packageResolution = packageResolution,
        engineLogger = engineLogger,
        submissionInfo = None,
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
  )(implicit loggingContext: LoggingContext): Result[Unit] = {
    // reinterpret
    for {
      result <- replay(
        submitters,
        tx,
        ledgerEffectiveTime,
        participantId,
        preparationTime,
        submissionSeed,
      )
      (rtx, _) = result
      validationResult <-
        transaction.Validation
          .isReplayedBy(tx, rtx)
          .fold(
            e => ResultError(Error.Validation.ReplayMismatch(e)),
            _ => ResultDone.Unit,
          )
    } yield validationResult
  }

  /** Builds a GlobalKey based on a normalised representation of the provided key
    * @param templateId - the templateId associated with the contract key
    * @param key - a representation of the key that may be un-normalized
    */
  def buildGlobalKey(templateId: Identifier, key: Value): Result[GlobalKey] = {
    preprocessor.buildGlobalKey(templateId: Identifier, key: Value)
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
      disclosures: ImmArray[speedy.DisclosedContract] = ImmArray.empty,
      ledgerTime: Time.Timestamp,
      preparationTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      engineLogger: Option[EngineLogger] = None,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] =
    for {
      sexpr <- runCompilerSafely(
        NameOf.qualifiedNameOfCurrentFunc,
        compiledPackages.compiler.unsafeCompile(commands, disclosures),
      )
      result <- interpretExpression(
        validating,
        submitters,
        readAs,
        sexpr,
        ledgerTime,
        preparationTime,
        seeding,
        packageResolution,
        engineLogger,
        submissionInfo,
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
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      engineLogger: Option[EngineLogger] = None,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    val machine = UpdateMachine(
      compiledPackages = compiledPackages,
      preparationTime = preparationTime,
      initialSeeding = seeding,
      expr = SEApp(sexpr, Array(SValue.SToken)),
      committers = submitters,
      readAs = readAs,
      authorizationChecker = config.authorizationChecker,
      validating = validating,
      traceLog = EngineLogger.toTraceLog(engineLogger),
      contractKeyUniqueness = config.contractKeyUniqueness,
      contractIdVersion = config.createContractsWithContractIdVersion,
      packageResolution = packageResolution,
      limits = config.limits,
      iterationsBetweenInterruptions = config.iterationsBetweenInterruptions,
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
    requireContractIdSuffix = false,
  )

  private[engine] def interpretLoop(
      machine: UpdateMachine,
      time: Time.Timestamp,
      submissionInfo: Option[Engine.SubmissionInfo] = None,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val abort = () => {
      machine.abort()
      Some(machine.transactionTrace(config.transactionTraceMaxLength))
    }

    def finish: Result[(SubmittedTransaction, Tx.Metadata)] =
      machine.finish match {
        case Right(
              UpdateMachine.Result(tx, _, nodeSeeds, globalKeyMapping, disclosedCreateEvents)
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
              disclosedEvents = disclosedCreateEvents,
            )

            config.profileDir.foreach { dir =>
              val desc = Engine.profileDesc(tx)
              val profileFile = dir.resolve(s"${meta.preparationTime}-$desc.json")
              machine.profile.name = s"${meta.preparationTime}-$desc"
              machine.profile.writeSpeedscopeJson(profileFile)
            }
            val snapshotResult = config.snapshotDir.zip(submissionInfo).flatMap {
              case (dir, Engine.SubmissionInfo(participantId, submissionSeed, submitters)) =>
                val snapshotFile = dir.resolve(s"snapshot-$participantId.bin")
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
                ResultDone((tx, meta))
            }
          }
        case Left(err) =>
          handleError(err, None)
      }

    @scala.annotation.tailrec
    def loop: Result[(SubmittedTransaction, Tx.Metadata)] = {
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
                { coinst =>
                  callback(coinst.toThinInstance)
                  interpretLoop(machine, time, submissionInfo)
                },
              )

            case Question.Update.NeedUpgradeVerification(
                  coid,
                  signatories,
                  observers,
                  keyOpt,
                  callback,
                ) =>
              ResultNeedUpgradeVerification(
                coid,
                signatories,
                observers,
                keyOpt,
                { x =>
                  callback(x)
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
    val darPackages = dar.all.toMap
    val mainPackageId = dar.main._1
    val mainPackageDependencies = dar.main._2.directDeps

    sealed abstract class PackageClassification
    final case class ExtraPackage(pkgId: PackageId, pkg: Package) extends PackageClassification
    final case class DependentPackage(pkgId: PackageId) extends PackageClassification
    final case class MissingPackage(pkgId: PackageId) extends PackageClassification

    @tailrec
    def calculateDependencyInformation(
        pkgIds: Set[PackageId],
        pkgClassification: Map[PackageId, PackageClassification],
    ): (Set[PackageId], Set[PackageId], Set[PackageId]) = {
      val unclassifiedPkgIds = pkgIds.collect {
        case id
            if !pkgClassification.contains(id) || pkgClassification(id)
              .isInstanceOf[ExtraPackage] =>
          id
      }

      if (unclassifiedPkgIds.isEmpty) {
        val result = pkgClassification.values.toSet
        val knownDeps = result.collect { case DependentPackage(id) => id }
        val missingDeps = result.collect { case MissingPackage(id) => id }
        val extraDeps = result.collect { case ExtraPackage(id, _) => id }

        (knownDeps, missingDeps, extraDeps)
      } else {
        val (knownPkgIds, missingPkdIds) =
          unclassifiedPkgIds.partition(id => pkgClassification.contains(id))
        val missingDeps = missingPkdIds.map(id => id -> MissingPackage(id)).toMap
        val knownDeps = knownPkgIds.map(id => id -> DependentPackage(id)).toMap
        // There is no point in searching through missing package Ids!
        val directDeps =
          knownPkgIds.flatMap(id => pkgClassification(id).asInstanceOf[ExtraPackage].pkg.directDeps)

        calculateDependencyInformation(directDeps, pkgClassification ++ knownDeps ++ missingDeps)
      }
    }

    for {
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
      // missingDeps are transitive dependencies (of the Dar main package) that are missing from the Dar manifest
      (transitiveDeps, missingDeps, unusedDeps) = calculateDependencyInformation(
        mainPackageDependencies,
        darPackages.map { case (id, pkg) => id -> ExtraPackage(id, pkg) },
      )
      // extraDeps are unused Dar manifest package IDs that are not stable packages and not the main package ID
      extraDeps = unusedDeps.diff(Set(mainPackageId) union stablePackageIds)
      _ <- Either.cond(
        missingDeps.isEmpty && extraDeps.isEmpty,
        (),
        Error.Package.DarSelfConsistency(mainPackageId, transitiveDeps, missingDeps, extraDeps),
      )
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
    } yield Versioned(version, r.toNormalizedValue(version))
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

  def DevEngine(majorLanguageVersion: LanguageMajorVersion): Engine = new Engine(
    EngineConfig(allowedLanguageVersions = LanguageVersion.AllVersions(majorLanguageVersion))
  )
}
