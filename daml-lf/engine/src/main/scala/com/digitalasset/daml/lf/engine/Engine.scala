// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.data.Ref.{PackageId, ParticipantId, Party}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{InitialSeeding, PartialTransaction, Pretty, SError}
import com.daml.lf.speedy.SExpr.{SExpr, SEApp, SEValue}
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.{
  Node,
  SubmittedTransaction,
  Versioned,
  VersionedTransaction,
  Transaction => Tx,
}
import java.nio.file.Files
import com.daml.lf.value.Value.{ValueRecord, ValueContractId, VersionedContractInstance, ContractId}

import com.daml.lf.language.{PackageInterface, LanguageVersion, LookupError, StablePackage}
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
class Engine(val config: EngineConfig = Engine.StableConfig) {

  config.profileDir.foreach(Files.createDirectories(_))

  private[this] val compiledPackages = ConcurrentCompiledPackages(config.getCompilerConfig)

  private[this] val stablePackageIds = StablePackage.ids(config.allowedLanguageVersions)

  private[engine] val preprocessor =
    new preprocessing.Preprocessor(
      compiledPackages = compiledPackages,
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
      cmds: ApiCommands,
      disclosures: ImmArray[DisclosedContract] = ImmArray.empty,
      participantId: ParticipantId,
      submissionSeed: crypto.Hash,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {

    // TODO (drsk) remove this assertion once disclosed contracts feature becomes stable.
    // https://github.com/digital-asset/daml/issues/13952.
    assert(disclosures.isEmpty || config.allowedLanguageVersions.contains(LanguageVersion.v1_dev))
    val submissionTime = cmds.ledgerEffectiveTime

    for {
      processedCmds <- preprocessor.preprocessApiCommands(cmds.commands)
      processedDiscs <- preprocessor.preprocessDisclosedContracts(disclosures)
      res <-
        interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = readAs,
          commands = processedCmds,
          disclosures = processedDiscs,
          ledgerTime = cmds.ledgerEffectiveTime,
          submissionTime = submissionTime,
          seeding = Engine.initialSeeding(submissionSeed, participantId, submissionTime),
        ) map { case (tx, meta) => tx -> meta.copy(submissionSeed = Some(submissionSeed)) }
    } yield res
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
      command: ReplayCommand,
      nodeSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      ledgerEffectiveTime: Time.Timestamp,
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
        disclosures = ImmArray.empty,
        sexpr = sexpr,
        ledgerTime = ledgerEffectiveTime,
        submissionTime = submissionTime,
        seeding = InitialSeeding.RootNodeSeeds(ImmArray(nodeSeed)),
      )
    } yield result

  def replay(
      submitters: Set[Party],
      tx: SubmittedTransaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionTime: Time.Timestamp,
      submissionSeed: crypto.Hash,
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
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] =
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
        disclosures,
        ledgerTime,
        submissionTime,
        seeding,
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
      disclosures: ImmArray[speedy.DisclosedContract],
      ledgerTime: Time.Timestamp,
      submissionTime: Time.Timestamp,
      seeding: speedy.InitialSeeding,
  )(implicit loggingContext: LoggingContext): Result[(SubmittedTransaction, Tx.Metadata)] = {
    val machine = Machine(
      compiledPackages = compiledPackages,
      submissionTime = submissionTime,
      initialSeeding = seeding,
      expr = SEApp(sexpr, Array(SEValue.Token)),
      committers = submitters,
      readAs = readAs,
      validating = validating,
      contractKeyUniqueness = config.contractKeyUniqueness,
      limits = config.limits,
    )

    for {
      discTable <- Engine.buildDiscTable(machine, disclosures)
      res <- interpretLoop(machine, discTable, ledgerTime)
    } yield res
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private[engine] def deps(tx: VersionedTransaction): Result[Set[PackageId]] = {
    val nodePkgIds =
      tx.nodes.values.collect { case node: Node.Action => node.templateId.packageId }.toSet
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

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private[engine] def interpretLoop(
      machine: Machine,
      disclosures: Engine.DisclosureTable,
      time: Time.Timestamp,
  ): Result[(SubmittedTransaction, Tx.Metadata)] = machine.withOnLedger("Daml Engine") { onLedger =>
    var finished: Boolean = false
    def detailMsg = Some(
      s"Last location: ${Pretty.prettyLoc(machine.lastLocation).render(80)}, partial transaction: ${onLedger.ptxInternal.nodesToString}"
    )
    while (!finished) {
      machine.run() match {
        case SResultFinalValue(_) => finished = true

        case SResultError(err) =>
          err match {
            case SError.SErrorDamlException(error) =>
              return ResultError(Error.Interpretation.DamlException(error), detailMsg)
            case err @ SError.SErrorCrash(where, reason) =>
              return ResultError(Error.Interpretation.Internal(where, reason, Some(err)))
          }
        case SResultNeedPackage(pkgId, context, callback) =>
          return Result.needPackage(
            pkgId,
            context,
            pkg => {
              compiledPackages.addPackage(pkgId, pkg).flatMap { _ =>
                callback(compiledPackages)
                interpretLoop(machine, disclosures, time)
              }
            },
          )

        case SResultNeedContract(contractId, _, callback) => {
          def continueWithContract = (coinst: VersionedContractInstance) => {
            callback(coinst)
            interpretLoop(machine, disclosures, time)
          }
          disclosures.contractById.get(contractId) match {
            case None =>
              return Result.needContract(
                contractId,
                continueWithContract,
              )
            case Some(coinst) => return continueWithContract(coinst)
          }
        }

        case SResultNeedTime(callback) =>
          callback(time)

        case SResultNeedKey(gk, _, cb) =>
          def continueWithCoid = (result: Option[ContractId]) => {
            discard[Boolean](cb(result))
            interpretLoop(machine, disclosures, time)
          }

          disclosures.contractIdByKey.get(gk.globalKey.hash) match {
            case None =>
              return ResultNeedKey(
                gk,
                continueWithCoid,
              )
            // TODO (drsk) validate key hash. https://github.com/digital-asset/daml/issues/13897
            case Some(coid) => return continueWithCoid(Some(coid))
          }

        case err @ (_: SResultScenarioSubmit | _: SResultScenarioPassTime |
            _: SResultScenarioGetParty) =>
          return ResultError(
            Error.Interpretation.Internal(
              NameOf.qualifiedNameOfCurrentFunc,
              s"unexpected ${err.getClass.getSimpleName}",
              None,
            )
          )
      }
    }

    onLedger.finish match {
      case PartialTransaction.CompleteTransaction(tx, _, nodeSeeds, globalKeyMapping) =>
        deps(tx).flatMap { deps =>
          val meta = Tx.Metadata(
            submissionSeed = None,
            submissionTime = onLedger.ptxInternal.submissionTime,
            usedPackages = deps,
            dependsOnTime = onLedger.dependsOnTime,
            nodeSeeds = nodeSeeds,
            globalKeyMapping = globalKeyMapping,
            disclosures = disclosures.allDisclosures,
          )
          config.profileDir.foreach { dir =>
            val desc = Engine.profileDesc(tx)
            machine.profile.name = s"${meta.submissionTime}-$desc"
            val profileFile = dir.resolve(s"${meta.submissionTime}-$desc.json")
            machine.profile.writeSpeedscopeJson(profileFile)
          }
          ResultDone((tx, meta))
        }
      case PartialTransaction.IncompleteTransaction(ptx) =>
        ResultError(
          Error.Interpretation.Internal(
            NameOf.qualifiedNameOfCurrentFunc,
            s"Interpretation error: ended with partial result: $ptx",
            None,
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

  private def StableConfig =
    EngineConfig(allowedLanguageVersions = LanguageVersion.StableVersions)

  def StableEngine(): Engine = new Engine(StableConfig)

  def DevEngine(): Engine = new Engine(
    StableConfig.copy(allowedLanguageVersions = LanguageVersion.DevVersions)
  )

  private[engine] case class DisclosureTable(
      allDisclosures: ImmArray[Versioned[DisclosedContract]],
      contractIdByKey: Map[crypto.Hash, ContractId],
      contractById: Map[ContractId, VersionedContractInstance],
  )

  private[engine] def buildDiscTable(
      machine: Machine,
      disclosures: ImmArray[speedy.DisclosedContract],
  ): Result[DisclosureTable] = {
    val acc = disclosures.foldLeft(
      (
        Map.empty[ContractId, VersionedContractInstance],
        Map.empty[crypto.Hash, ContractId],
        List.empty[Versioned[DisclosedContract]],
      )
    ) { case ((m1, m2, ds), d) =>
      val arg = machine.normValue(d.templateId, d.argument)
      val coid = machine.normValue(d.templateId, d.contractId)
      val version = machine.tmplId2TxVersion(d.templateId)

      // check for well typed contract argument
      (coid, arg) match {
        case (ValueContractId(coid), r @ ValueRecord(Some(d.templateId), _)) =>
          // check for duplicate contract ids
          m1.get(coid) match {
            case Some(_) =>
              throw (Error.Preprocessing.BadDisclosedContract("Duplicate disclosed contract."))
            case None =>
              val contractInst = VersionedContractInstance(
                template = d.templateId,
                arg = Versioned(version, r),
                agreementText = "",
              )
              val m1_prime = m1 + (coid -> contractInst)
              val d_prime = Versioned(
                version,
                DisclosedContract(
                  templateId = d.templateId,
                  argument = r,
                  contractId = coid,
                  metadata = d.metadata,
                ),
              )
              val ds_prime = d_prime :: ds
              d.metadata.keyHash match {
                case Some(hash) =>
                  // check for duplicate contract key hashes
                  m2.get(hash) match {
                    case Some(_) =>
                      throw (Error.Preprocessing.BadDisclosedContract(
                        "Duplicate key for disclosed contract."
                      ))
                    case None => (m1_prime, m2 + (hash -> coid), ds_prime)
                  }
                case None => (m1_prime, m2, ds_prime)
              }
          }
        case _ =>
          throw (Error.Preprocessing.BadDisclosedContract("Disclosed contract is not well-typed."))
      }
    }
    ResultDone(
      DisclosureTable(
        allDisclosures = ImmArray.from(acc._3),
        contractById = acc._1,
        contractIdByKey = acc._2,
      )
    )

  }
}
