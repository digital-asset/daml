// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.data.Ref.{PackageId, ParticipantId, Party}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{InitialSeeding, PartialTransaction, Pretty, SError, SExpr}
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.{SubmittedTransaction, Transaction => Tx}
import com.daml.lf.transaction.Node._
import java.nio.file.Files

import com.daml.lf.language.{PackageInterface, LanguageVersion, LookupError, StablePackages}
import com.daml.lf.validation.Validation
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
class Engine(val config: EngineConfig = Engine.StableConfig) {

  config.profileDir.foreach(Files.createDirectories(_))

  private[this] val compiledPackages = ConcurrentCompiledPackages(config.getCompilerConfig)

  private[engine] val preprocessor =
    new preprocessing.Preprocessor(
      compiledPackages = compiledPackages,
      forbidV0ContractId = config.forbidV0ContractId,
      requireV1ContractIdSuffix = config.requireSuffixedGlobalContractId,
    )

  def info = new EngineInfo(config)

  /** Executes commands `cmds` under the authority of `submitters`, with additional readers `readAs`,
    * and returns one of the following:
    * <ul>
    * <li> `ResultDone(tx)` if `cmds` could be successfully executed, where `tx` is the resulting transaction.
    *      The transaction `tx` conforms to the Daml model consisting of the packages that have been supplied via
    *      `ResultNeedPackage.resume` to this [[Engine]].
    *      The transaction `tx` is internally consistent.
    * </li>
    * <li> `ResultNeedContract(contractId, resume)` if the contract referenced by `contractId` is needed to execute
    *      `cmds`.
    * </li>
    * <li> `ResultNeedPackage(packageId, resume)` if the package referenced by `packageId` is needed to execute `cmds`.
    * </li>
    * <li> `ResultError` if the execution of `cmds` fails.
    *      The execution may fail due to an error during Daml evaluation (e.g. execution of "abort") or
    *      because the caller has not provided a required contract instance or package.
    * </li>
    * </ul>
    *
    * [[transactionSeed]] is the master hash used to derive node and contractId discriminator.
    * If left undefined, no discriminator will be generated.
    *
    * This method does perform authorization checks
    *
    * The resulting transaction is annotated with packages required to validate it.
    */
  def submit(
      submitters: Set[Party],
      readAs: Set[Party],
      cmds: Commands,
      participantId: ParticipantId,
      submissionSeed: crypto.Hash,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val submissionTime = cmds.ledgerEffectiveTime
    preprocessor
      .preprocessCommands(cmds.commands)
      .flatMap { processedCmds =>
        interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = readAs,
          commands = processedCmds,
          ledgerTime = cmds.ledgerEffectiveTime,
          submissionTime = submissionTime,
          seeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime),
        ) map { case (tx, meta) =>
          // Annotate the transaction with the package dependencies. Since
          // all commands are actions on a contract template, with a fully typed
          // argument, we only need to consider the templates mentioned in the command
          // to compute the full dependencies.
          val deps = processedCmds.foldLeft(Set.empty[PackageId]) { (pkgIds, cmd) =>
            val pkgId = cmd.templateId.packageId
            val transitiveDeps =
              compiledPackages
                .getPackageDependencies(pkgId)
                .getOrElse(sys.error(s"INTERNAL ERROR: Missing dependencies of package $pkgId"))
            (pkgIds + pkgId) union transitiveDeps
          }
          tx -> meta.copy(submissionSeed = Some(submissionSeed), usedPackages = deps)
        }
      }
  }

  /** Behaves like `submit`, but it takes a single command argument.
    * It can be used to reinterpret partially an already interpreted transaction.
    *
    * If the command would fail with an unhandled exception, we return a transaction containing a
    * single rollback node. (This is achieving by compiling with `unsafeCompileForReinterpretation`
    * which wraps the command with a catch-everything exception handler.)
    *
    * [[nodeSeed]] is the seed of the Create and Exercise node as generated during submission.
    * If undefined the contract IDs are derive using V0 scheme.
    * The value of [[nodeSeed]] does not matter for other kind of nodes.
    *
    * The reinterpretation does not recompute the package dependencies, so the field `usedPackages` in the
    * `Tx.MetaData` component of the output is always set to `empty`.
    */
  def reinterpret(
      submitters: Set[Party],
      command: Command,
      nodeSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
  ): Result[(SubmittedTransaction, Tx.Metadata)] =
    for {
      speedyCommand <- preprocessor.preprocessCommand(command)
      sexpr = compiledPackages.compiler.unsafeCompileForReinterpretation(speedyCommand)
      // reinterpret is never used for submission, only for validation.
      result <- interpretExpression(
        validating = true,
        submitters = submitters,
        readAs = Set.empty,
        sexpr = sexpr,
        ledgerTime = ledgerEffectiveTime,
        submissionTime = submissionTime,
        seeding = InitialSeeding.RootNodeSeeds(ImmArray(nodeSeed)),
      )
      (tx, meta) = result
    } yield (tx, meta)

  def replay(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
  ): Result[(SubmittedTransaction, Tx.Metadata)] =
    for {
      commands <- preprocessor.translateTransactionRoots(tx)
      result <- interpretCommands(
        validating = true,
        submitters = submitters,
        readAs = Set.empty,
        commands = commands,
        ledgerTime = ledgerEffectiveTime,
        submissionTime = submissionTime,
        seeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime),
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
  ): Result[Unit] = {
    //reinterpret
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
  private[lf] def runSafely[X](
      funcName: String
  )(run: => Result[X]): Result[X] = {
    def start: Result[X] =
      try {
        run
      } catch {
        // The two following error should be prevented by the type checking does by translateCommand
        // so it’s an internal error.
        case speedy.Compiler.PackageNotFound(pkgId, context) =>
          ResultError(
            Error.Preprocessing.Internal(
              funcName,
              s"CompilationError: " + LookupError.MissingPackage.pretty(pkgId, context),
            )
          )
        case speedy.Compiler.CompilationError(error) =>
          ResultError(Error.Preprocessing.Internal(funcName, s"CompilationError: $error"))
      }
    start
  }

  // command-list compilation, followed by interpretation
  private[engine] def interpretCommands(
      validating: Boolean,
      submitters: Set[Party],
      readAs: Set[Party],
      commands: ImmArray[speedy.Command],
      ledgerTime: Time.Timestamp,
      submissionTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val sexpr = compiledPackages.compiler.unsafeCompile(commands)
    interpretExpression(
      validating,
      submitters,
      readAs,
      sexpr,
      ledgerTime,
      submissionTime,
      seeding,
    )
  }

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
  ): Result[(SubmittedTransaction, Tx.Metadata)] =
    runSafely(NameOf.qualifiedNameOfCurrentFunc) {
      val machine = Machine(
        compiledPackages = compiledPackages,
        submissionTime = submissionTime,
        initialSeeding = seeding,
        expr = SExpr.SEApp(sexpr, Array(SExpr.SEValue.Token)),
        committers = submitters,
        readAs = readAs,
        validating = validating,
        contractKeyUniqueness = config.contractKeyUniqueness,
      )
      interpretLoop(machine, ledgerTime)
    }

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private[engine] def interpretLoop(
      machine: Machine,
      time: Time.Timestamp,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = machine.withOnLedger("Daml Engine") { onLedger =>
    var finished: Boolean = false
    def detailMsg = Some(
      s"Last location: ${Pretty.prettyLoc(machine.lastLocation).render(80)}, partial transaction: ${onLedger.ptxInternal.nodesToString}"
    )
    while (!finished) {
      machine.run() match {
        case SResultFinalValue(_) => finished = true

        case SResultError(SError.SErrorDamlException(error)) =>
          return ResultError(Error.Interpretation.DamlException(error), detailMsg)

        case SResultError(err) =>
          err match {
            case SError.SErrorCrash(where, reason) =>
              discard[Error.Interpretation.Internal](Error.Interpretation.Internal(where, reason))
            case SError.SErrorDamlException(error) =>
              discard[Error.Interpretation.DamlException](Error.Interpretation.DamlException(error))
          }
        case SResultNeedPackage(pkgId, context, callback) =>
          return Result.needPackage(
            pkgId,
            context,
            pkg => {
              compiledPackages.addPackage(pkgId, pkg).flatMap { _ =>
                callback(compiledPackages)
                interpretLoop(machine, time)
              }
            },
          )

        case SResultNeedContract(contractId, _, _, callback) =>
          return Result.needContract(
            contractId,
            { coinst =>
              callback(coinst)
              interpretLoop(machine, time)
            },
          )

        case SResultNeedTime(callback) =>
          callback(time)

        case SResultNeedKey(gk, _, cb) =>
          return ResultNeedKey(
            gk,
            { result =>
              discard[Boolean](cb(result))
              interpretLoop(machine, time)
            },
          )

        case err @ (_: SResultScenarioSubmit | _: SResultScenarioPassTime |
            _: SResultScenarioGetParty) =>
          return ResultError(
            Error.Interpretation.Internal(
              NameOf.qualifiedNameOfCurrentFunc,
              s"unexpected ${err.getClass.getSimpleName}",
            )
          )
      }
    }

    onLedger.finish match {
      case PartialTransaction.CompleteTransaction(tx, _, nodeSeeds) =>
        val meta = Tx.Metadata(
          submissionSeed = None,
          submissionTime = onLedger.ptxInternal.submissionTime,
          usedPackages = Set.empty,
          dependsOnTime = onLedger.dependsOnTime,
          nodeSeeds = nodeSeeds,
        )
        config.profileDir.foreach { dir =>
          val desc = Engine.profileDesc(tx)
          machine.profile.name = s"${meta.submissionTime}-$desc"
          val profileFile = dir.resolve(s"${meta.submissionTime}-$desc.json")
          machine.profile.writeSpeedscopeJson(profileFile)
        }
        ResultDone((tx, meta))
      case PartialTransaction.IncompleteTransaction(ptx) =>
        ResultError(
          Error.Interpretation.Internal(
            NameOf.qualifiedNameOfCurrentFunc,
            s"Interpretation error: ended with partial result: $ptx",
          )
        )
    }
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
              if !StablePackages.Ids.contains(pkgId) && !config.allowedLanguageVersions
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
      interface = PackageInterface(pkgs)
      _ <- {
        pkgs.iterator
          // we trust already loaded packages
          .collect {
            case (pkgId, pkg) if !compiledPackages.packageIds.contains(pkgId) =>
              Validation.checkPackage(interface, pkgId, pkg)
          }
          .collectFirst { case Left(err) => Error.Package.Validation(err) }
      }.toLeft(())

    } yield ()
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

  private def profileDesc(tx: Tx.Transaction): String = {
    if (tx.roots.length == 1) {
      val makeDesc = (kind: String, tmpl: Ref.Identifier, extra: Option[String]) =>
        s"$kind:${tmpl.qualifiedName.name}${extra.map(extra => s":$extra").getOrElse("")}"
      tx.nodes.get(tx.roots(0)).toList.head match {
        case _: NodeRollback => "rollback"
        case create: NodeCreate => makeDesc("create", create.templateId, None)
        case exercise: NodeExercises =>
          makeDesc("exercise", exercise.templateId, Some(exercise.choiceId))
        case fetch: NodeFetch => makeDesc("fetch", fetch.templateId, None)
        case lookup: NodeLookupByKey => makeDesc("lookup", lookup.templateId, None)
      }
    } else {
      s"compound:${tx.roots.length}"
    }
  }

  private def StableConfig =
    EngineConfig(allowedLanguageVersions = LanguageVersion.StableVersions)

  def StableEngine(): Engine = new Engine(StableConfig)

  def DevEngine(): Engine = new Engine(
    StableConfig.copy(allowedLanguageVersions = LanguageVersion.DevVersions)
  )

}
