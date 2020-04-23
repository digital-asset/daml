// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.data.Ref.{PackageId, ParticipantId, Party}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.Compiler
import com.daml.lf.speedy.Pretty
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.Transaction
import com.daml.lf.transaction.Node._
import com.daml.lf.value.Value
import com.daml.lf.speedy.{Command => SpeedyCommand}

/**
  * Allows for evaluating [[Commands]] and validating [[Transaction]]s.
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
final class Engine {
  private[this] val _compiledPackages = ConcurrentCompiledPackages()
  private[this] val _preprocessor = new preprocessing.Preprocessor(_compiledPackages)

  /**
    * Executes commands `cmds` under the authority of `cmds.submitter` and returns one of the following:
    * <ul>
    * <li> `ResultDone(tx)` if `cmds` could be successfully executed, where `tx` is the resulting transaction.
    *      The transaction `tx` conforms to the DAML model consisting of the packages that have been supplied via
    *      `ResultNeedPackage.resume` to this [[Engine]].
    *      The transaction `tx` is internally consistent.
    * </li>
    * <li> `ResultNeedContract(contractId, resume)` if the contract referenced by `contractId` is needed to execute
    *      `cmds`.
    * </li>
    * <li> `ResultNeedPackage(packageId, resume)` if the package referenced by `packageId` is needed to execute `cmds`.
    * </li>
    * <li> `ResultError` if the execution of `cmds` fails.
    *      The execution may fail due to an error during DAML evaluation (e.g. execution of "abort") or
    *      because the caller has not provided a required contract instance or package.
    * </li>
    * </ul>
    *
    *
    * [[transactionSeed]] is the master hash used to derive node and contractId discriminator.
    * If let undefined, no discriminator will be generated.
    *
    * This method does NOT perform authorization checks; ResultDone can contain a transaction that's not well-authorized.
    *
    * The resulting transaction is annotated with packages required to validate it.
    */
  def submit(
      cmds: Commands,
      participantId: ParticipantId,
      submissionSeed: Option[crypto.Hash],
  ): Result[(Transaction.Transaction, Transaction.Metadata)] = {
    val submissionTime = cmds.ledgerEffectiveTime
    _preprocessor
      .preprocessCommands(cmds.commands)
      .flatMap { processedCmds =>
        ShouldCheckSubmitterInMaintainers(_compiledPackages, cmds).flatMap {
          checkSubmitterInMaintainers =>
            interpretCommands(
              validating = false,
              checkSubmitterInMaintainers = checkSubmitterInMaintainers,
              submitters = Set(cmds.submitter),
              commands = processedCmds,
              ledgerTime = cmds.ledgerEffectiveTime,
              transactionSeedAndSubmissionTime = submissionSeed.map(seed =>
                crypto.Hash
                  .deriveTransactionSeed(seed, participantId, submissionTime) -> submissionTime),
            ) map {
              case (tx, dependsOnTime) =>
                // Annotate the transaction with the package dependencies. Since
                // all commands are actions on a contract template, with a fully typed
                // argument, we only need to consider the templates mentioned in the command
                // to compute the full dependencies.
                val deps = processedCmds.foldLeft(Set.empty[PackageId]) { (pkgIds, cmd) =>
                  val pkgId = cmd.templateId.packageId
                  val transitiveDeps =
                    _compiledPackages
                      .getPackageDependencies(pkgId)
                      .getOrElse(
                        sys.error(s"INTERNAL ERROR: Missing dependencies of package $pkgId"))
                  (pkgIds + pkgId) union transitiveDeps
                }
                tx -> Transaction.Metadata(
                  submissionTime = submissionTime,
                  usedPackages = deps,
                  dependsOnTime = dependsOnTime,
                )
            }
        }
      }
  }

  /**
    * Behaves like `submit`, but it takes GenNode arguments instead of a Commands argument.
    * That is, it can be used to reinterpret an already interpreted transaction (since it consists of GenNodes).
    * Formally, the following is guaranteed to hold for all pcs, pkgs, and keys, when evaluated on the same Engine:
    * evaluate(submit(cmds)) = ResultDone(tx) ==> evaluate(reinterpret(cmds.submitters, txRoots, cmds.ledgerEffectiveTime)) === ResultDone(tx)
    * where:
    *   evaluate(result) = result.consume(pcs, pkgs, keys)
    *   txRoots = tx.roots.map(id => tx.nodes.get(id).get).toSeq
    *   tx === tx' if tx and tx' are equivalent modulo a renaming of node and relative contract IDs
    *
    * Moreover, if the transaction tx is valid at time leTime, n belongs to tx.nodes, and subtx is the subtransaction of
    * tx rooted at n, the following holds:
    * evaluate(reinterpret(n.requiredAuthorizers, Seq(n), leTime) === subtx
    *
    * In addition to the errors returned by `submit`, reinterpretation fails with a `ValidationError` whenever `nodes`
    * contain a relative contract ID, either as the target contract of a fetch, or as an argument to a
    * create or an exercise choice.
    *
    * [[transactionSeed]] is the master hash te be used to derive node and contractId discriminator.
    * If let undefined, no discriminator will be generated.
    */
  def reinterpret(
      rootSeedAndSubmissionTime: Option[(crypto.Hash, Time.Timestamp)],
      submitters: Set[Party],
      nodes: Seq[GenNode.WithTxValue[Value.NodeId, Value.ContractId]],
      ledgerEffectiveTime: Time.Timestamp,
  ): Result[(Transaction.Transaction, Boolean)] =
    for {
      commands <- Result.sequence(ImmArray(nodes).map(_preprocessor.translateNode))
      checkSubmitterInMaintainers <- ShouldCheckSubmitterInMaintainers(
        _compiledPackages,
        commands.map(_.templateId))
      // reinterpret is never used for submission, only for validation.
      result <- interpretCommands(
        validating = true,
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        submitters = submitters,
        commands = commands,
        ledgerTime = ledgerEffectiveTime,
        rootSeedAndSubmissionTime,
      )
    } yield result

  /**
    * Check if the given transaction is a valid result of some single-submitter command.
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
      tx: Transaction.Transaction,
      ledgerEffectiveTime: Time.Timestamp,
      participantId: Ref.ParticipantId,
      submissionSeedAndTime: Option[(crypto.Hash, Time.Timestamp)] = None,
  ): Result[Unit] = {
    import scalaz.std.option._
    import scalaz.syntax.traverse.ToTraverseOps
    val transactionSeedAndSubmissionTime = submissionSeedAndTime.map {
      case (seed, time) =>
        crypto.Hash.deriveTransactionSeed(seed, participantId, time) -> time
    }
    //reinterpret
    for {
      requiredAuthorizers <- tx.roots
        .traverseU(nid => tx.nodes.get(nid).map(_.requiredAuthorizers)) match {
        case None => ResultError(ValidationError(s"invalid roots for transaction $tx"))
        case Some(nodes) => ResultDone(nodes)
      }

      // We must be able to validate empty transactions, hence use an option
      submittersOpt <- requiredAuthorizers.foldLeft[Result[Option[Set[Party]]]](ResultDone(None)) {
        case (ResultDone(None), authorizers2) => ResultDone(Some(authorizers2))
        case (ResultDone(Some(authorizers1)), authorizers2) if authorizers1 == authorizers2 =>
          ResultDone(Some(authorizers2))
        case _ =>
          ResultError(ValidationError(s"Transaction's roots have different authorizers: $tx"))
      }

      _ <- if (submittersOpt.exists(_.size != 1))
        ResultError(ValidationError(s"Transaction's roots do not have exactly one authorizer: $tx"))
      else ResultDone(())

      // For empty transactions, use an empty set of submitters
      submitters = submittersOpt.getOrElse(Set.empty)

      commands <- _preprocessor.translateTransactionRoots(tx)
      checkSubmitterInMaintainers <- ShouldCheckSubmitterInMaintainers(
        _compiledPackages,
        commands.map(_._2.templateId))
      result <- interpretCommands(
        validating = true,
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        submitters = submitters,
        commands = commands.map(_._2),
        ledgerTime = ledgerEffectiveTime,
        transactionSeedAndSubmissionTime = transactionSeedAndSubmissionTime,
      )
      (rtx, _) = result
      validationResult <- if (tx isReplayedBy rtx) {
        ResultDone(())
      } else {
        ResultError(
          ValidationError(
            s"recreated and original transaction mismatch $tx expected, but $rtx is recreated"))
      }
    } yield validationResult
  }

  /** Interprets the given commands under the authority of @submitters
    *
    * Submitters are a set, in order to support interpreting subtransactions
    * (a subtransaction can be authorized by multiple parties).
    *
    * [[transactionSeed]] is the master hash used to derive node and contractId discriminator.
    * If let undefined, no discriminator will be generated.
    */
  private[engine] def interpretCommands(
      validating: Boolean,
      /* See documentation for `Speedy.Machine` for the meaning of this field */
      checkSubmitterInMaintainers: Boolean,
      submitters: Set[Party],
      commands: ImmArray[SpeedyCommand],
      ledgerTime: Time.Timestamp,
      transactionSeedAndSubmissionTime: Option[(crypto.Hash, Time.Timestamp)]
  ): Result[(Transaction.Transaction, Boolean)] = {
    val machine = Machine
      .build(
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        sexpr = Compiler(compiledPackages.packages).compile(commands),
        compiledPackages = _compiledPackages,
        transactionSeedAndSubmissionTime = transactionSeedAndSubmissionTime,
      )
      .copy(validating = validating, committers = submitters)
    interpretLoop(machine, ledgerTime)
  }

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Return"))
  private[engine] def interpretLoop(
      machine: Machine,
      time: Time.Timestamp
  ): Result[(Transaction.Transaction, Boolean)] = {
    while (!machine.isFinal) {
      machine.step() match {
        case SResultContinue =>
          ()

        case SResultError(err) =>
          return ResultError(
            Error(
              s"Interpretation error: ${Pretty.prettyError(err, machine.ptx).render(80)}",
              s"Last location: ${Pretty.prettyLoc(machine.lastLocation).render(80)}, partial transaction: ${machine.ptx.nodesToString}"
            ))

        case SResultNeedPackage(pkgId, callback) =>
          return Result.needPackage(
            pkgId,
            pkg => {
              _compiledPackages.addPackage(pkgId, pkg).flatMap {
                case _ =>
                  callback(_compiledPackages)
                  interpretLoop(machine, time)
              }
            }
          )

        case SResultNeedContract(contractId, _, _, _, cbPresent) =>
          return Result.needContract(
            contractId, { coinst =>
              cbPresent(coinst)
              interpretLoop(machine, time)
            }
          )

        case SResultNeedTime(callback) =>
          machine.dependsOnTime = true
          callback(time)

        case SResultNeedKey(gk, _, cb) =>
          return ResultNeedKey(
            gk,
            (
                result =>
                  if (cb(SKeyLookupResult(result)))
                    interpretLoop(machine, time)
                  else
                    ResultError(Error(s"dependency error: couldn't find key $gk"))
            )
          )

        case _: SResultScenarioCommit =>
          return ResultError(Error("unexpected ScenarioCommit"))

        case _: SResultScenarioInsertMustFail =>
          return ResultError(Error("unexpected ScenarioInsertMustFail"))
        case _: SResultScenarioMustFail =>
          return ResultError(Error("unexpected ScenarioMustFail"))
        case _: SResultScenarioPassTime =>
          return ResultError(Error("unexpected ScenarioPassTime"))
        case _: SResultScenarioGetParty =>
          return ResultError(Error("unexpected ScenarioGetParty"))
      }
    }

    machine.ptx.finish match {
      case Left(p) =>
        ResultError(Error(s"Interpretation error: ended with partial result: $p"))
      case Right(t) => ResultDone(t -> machine.dependsOnTime)
    }
  }

  def clearPackages(): Unit = _compiledPackages.clear()

  /** Note: it's important we return a [[com.daml.lf.CompiledPackages]],
    * and not a [[ConcurrentCompiledPackages]], otherwise people would be able
    * to modify them.
    */
  def compiledPackages(): CompiledPackages = _compiledPackages

  /** This function can be used to give a package to the engine pre-emptively,
    * rather than having the engine to ask about it through
    * [[ResultNeedPackage]].
    *
    * Returns a [[Result]] because the package might need another package to
    * be loaded.
    */
  def preloadPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    _compiledPackages.addPackage(pkgId, pkg)
}

object Engine {
  def apply(): Engine = new Engine()
}
