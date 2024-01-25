// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.data.Ref.{Identifier, PackageId, ParticipantId, Party}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{InitialSeeding, Pretty, Question, SError, SResult, SValue}
import com.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.daml.lf.speedy.Speedy.{Machine, PureMachine, UpdateMachine}
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.{
  Node,
  SubmittedTransaction,
  Versioned,
  VersionedTransaction,
  Transaction => Tx,
}

import java.nio.file.Files
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion, LookupError, PackageInterface}
import com.daml.lf.stablepackages.StablePackages
import com.daml.lf.validation.Validation
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

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
  * submission time. This generator does not have to be cryptographically secure.
  * <p>
  *
  * This class is thread safe as long `nextRandomInt` is.
  */
class Engine(val config: EngineConfig) {

  config.profileDir.foreach(Files.createDirectories(_))

  private[this] val compiledPackages = ConcurrentCompiledPackages(config.getCompilerConfig)

  private[this] val stablePackageIds = StablePackages.ids(config.allowedLanguageVersions)

  private[engine] val preprocessor =
    new preprocessing.Preprocessor(
      compiledPackages = compiledPackages,
      requireV1ContractIdSuffix = config.requireSuffixedGlobalContractId,
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
      disclosures: ImmArray[DisclosedContract] = ImmArray.empty,
      participantId: ParticipantId,
      submissionSeed: crypto.Hash,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    val submissionTime = cmds.ledgerEffectiveTime

    for {
      pkgResolution <- preprocessor.buildPackageResolution(packageMap, packagePreference)
      processedCmds <- preprocessor.preprocessApiCommands(pkgResolution, cmds.commands)
      processedDiscs <- preprocessor.preprocessDisclosedContracts(disclosures)
      result <-
        interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = readAs,
          commands = processedCmds,
          disclosures = processedDiscs,
          ledgerTime = cmds.ledgerEffectiveTime,
          submissionTime = submissionTime,
          seeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime),
          packageResolution = pkgResolution,
        )
      (tx, meta) = result
    } yield tx -> meta.copy(submissionSeed = Some(submissionSeed))
  }

  /** Behaves like `submit`, but it takes a single `ReplayCommand` (instead of `ApiCommands`) as input.
    * It can be used to reinterpret partially an already interpreted transaction.
    *
    * If the command would fail with an unhandled exception, we return a transaction containing a
    * single rollback node. (This is achieving by compiling with `unsafeCompileForReinterpretation`
    * which wraps the command with a catch-everything exception handler.)
    *
    * The reinterpretation does not recompute the package dependencies, so the field `usedPackages` in the
    * `Tx.MetaData` component of the output is always set to `empty`.
    *
    * @param nodeSeed the seed of the root node as generated during submission.
    *                 If undefined the contract IDs are derive using V0 scheme.
    *                 The value does not matter for other kind of nodes.
    * @param submissionTime the submission time used to compute contract IDs
    * @param ledgerEffectiveTime the ledger effective time used as a result of `getTime` during reinterpretation
    */
  def reinterpret(
      submitters: Set[Party],
      command: ReplayCommand,
      nodeSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
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
        submissionTime = submissionTime,
        seeding = InitialSeeding.RootNodeSeeds(ImmArray(nodeSeed)),
        packageResolution = packageResolution,
      )
    } yield result

  def replay(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
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
        submissionTime = submissionTime,
        seeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime),
        packageResolution = packageResolution,
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
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
  )(implicit loggingContext: LoggingContext): Result[Unit] = {
    // reinterpret
    for {
      result <- replay(
        submitters,
        tx,
        ledgerEffectiveTime,
        participantId,
        submissionTime,
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
      submissionTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
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
        submissionTime,
        seeding,
        packageResolution,
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
      submissionTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    val machine = UpdateMachine(
      compiledPackages = compiledPackages,
      submissionTime = submissionTime,
      initialSeeding = seeding,
      expr = SEApp(sexpr, Array(SValue.SToken)),
      committers = submitters,
      readAs = readAs,
      authorizationChecker = config.authorizationChecker,
      validating = validating,
      contractKeyUniqueness = config.contractKeyUniqueness,
      packageResolution = packageResolution,
      limits = config.limits,
      iterationsBetweenInterruptions = config.iterationsBetweenInterruptions,
    )
    interpretLoop(machine, ledgerTime)
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

  private def handleError(err: SError.SError, detailMsg: Option[String] = None): ResultError = {
    err match {
      case SError.SErrorDamlException(error) =>
        ResultError(Error.Interpretation.DamlException(error), detailMsg)
      case err @ SError.SErrorCrash(where, reason) =>
        ResultError(Error.Interpretation.Internal(where, reason, Some(err)))
    }
  }

  private[engine] def interpretLoop(
      machine: UpdateMachine,
      time: Time.Timestamp,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = {
    def detailMsg = Some(
      s"Last location: ${Pretty.prettyLoc(machine.getLastLocation).render(80)}, partial transaction: ${machine.nodesToString}"
    )

    def finish: Result[(SubmittedTransaction, Tx.Metadata)] =
      machine.finish match {
        case Right(
              UpdateMachine.Result(tx, _, nodeSeeds, globalKeyMapping, disclosedCreateEvents)
            ) =>
          deps(tx).flatMap { deps =>
            val meta = Tx.Metadata(
              submissionSeed = None,
              submissionTime = machine.submissionTime,
              usedPackages = deps,
              dependsOnTime = machine.getDependsOnTime,
              nodeSeeds = nodeSeeds,
              globalKeyMapping = globalKeyMapping,
              disclosedEvents = disclosedCreateEvents,
            )
            config.profileDir.foreach { dir =>
              val desc = Engine.profileDesc(tx)
              val profileFile = dir.resolve(s"${meta.submissionTime}-$desc.json")
              machine.profile.name = s"${meta.submissionTime}-$desc"
              machine.profile.writeSpeedscopeJson(profileFile)
            }
            ResultDone((tx, meta))
          }
        case Left(err) =>
          handleError(err)
      }

    @scala.annotation.tailrec
    def loop: Result[(SubmittedTransaction, Tx.Metadata)] = {
      machine.run() match {

        case SResultQuestion(question) =>
          question match {
            case Question.Update.NeedAuthority(holding @ _, requesting @ _, callback) =>
              ResultNeedAuthority(
                holding,
                requesting,
                { granted: Boolean =>
                  if (granted) {
                    callback()
                    interpretLoop(machine, time)
                  } else {
                    ResultError(
                      Error.Interpretation.DamlException(
                        interpretation.Error
                          .RejectedAuthorityRequest(holding = holding, requesting = requesting)
                      )
                    )
                  }
                },
              )

            case Question.Update.NeedPackageId(module @ _, pid0, callback) =>
              // TODO https://github.com/digital-asset/daml/issues/16154 (dynamic-exercise)
              // For now this just continues with the input package id
              callback(pid0)
              loop

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
                    interpretLoop(machine, time)
                  }
                },
              )

            case Question.Update.NeedContract(coid, _, callback) =>
              Result.needContract(
                coid,
                { coinst =>
                  callback(coinst.unversioned)
                  interpretLoop(machine, time)
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
                  interpretLoop(machine, time)
                },
              )

            case Question.Update.NeedKey(gk, _, callback) =>
              ResultNeedKey(
                gk,
                { coid: Option[ContractId] =>
                  discard[Boolean](callback(coid))
                  interpretLoop(machine, time)
                },
              )
          }

        case SResultInterruption =>
          ResultInterruption(() => interpretLoop(machine, time))

        case _: SResultFinal =>
          finish

        case SResultError(err) =>
          handleError(err, detailMsg)
      }
    }

    loop
  }

  def clearPackages(): Unit = compiledPackages.clear()

  /** Note: it's important we return a [[com.daml.lf.CompiledPackages]],
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

  /** This method checks a set of packages is self-consistent (it
    * contains all its dependencies), contains only well-formed
    * packages (See Daml-LF spec for more details) and uses only the
    * allowed language versions (as described by the engine
    * config).
    * This is not affected by [[config.packageValidation]] flag.
    * Package in [[pkgIds]] but not in [[pkgs]] are assumed to be
    * preloaded.
    */
  def validatePackages(
      pkgs: Map[PackageId, Package]
  ): Either[Error.Package.Error, Unit] = {
    for {
      _ <- pkgs
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
      pkgIds = pkgs.keySet
      missingDeps = pkgs.valuesIterator.flatMap(_.directDeps).toSet.filterNot(pkgIds)
      _ <- Either.cond(missingDeps.isEmpty, (), Error.Package.SelfConsistency(pkgIds, missingDeps))
      pkgInterface = PackageInterface(pkgs)
      _ <- {
        pkgs.iterator
          // we trust already loaded packages
          .collect {
            case (pkgId, pkg) if !compiledPackages.packageIds.contains(pkgId) =>
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
    def interpret(machine: PureMachine): Result[SValue] =
      machine.run() match {
        case SResultFinal(v) => ResultDone(v)
        case SResultError(err) => handleError(err, None)
        case SResult.SResultInterruption => ResultInterruption(() => interpret(machine))
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
      r <- interpret(machine)
      version = machine.tmplId2TxVersion(interfaceId)
    } yield Versioned(version, r.toNormalizedValue(version))
  }

}

object Engine {

  type Packages = Map[PackageId, Package]

  def initialSeeding(
      submissionSeed: crypto.Hash,
      participant: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
  ): InitialSeeding =
    InitialSeeding.TransactionSeed(
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
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
