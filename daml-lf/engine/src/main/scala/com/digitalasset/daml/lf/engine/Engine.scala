// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref.{PackageId, ParticipantId, Party}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.Pretty
import com.digitalasset.daml.lf.speedy.Speedy.Machine
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.speedy.{Command => SpeedyCommand}

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
  * This class is thread safe.
  */
final class Engine {
  private[this] val _compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()
  private[this] val _commandTranslation: CommandPreprocessor = new CommandPreprocessor(
    _compiledPackages)

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
  ): Result[Transaction.Transaction] = {
    _commandTranslation
      .preprocessCommands(cmds)
      .flatMap { processedCmds =>
        ShouldCheckSubmitterInMaintainers(_compiledPackages, cmds).flatMap {
          checkSubmitterInMaintainers =>
            interpretCommands(
              validating = false,
              checkSubmitterInMaintainers = checkSubmitterInMaintainers,
              submitters = Set(cmds.submitter),
              commands = processedCmds,
              time = cmds.ledgerEffectiveTime,
              transactionSeed = submissionSeed.map(
                crypto.Hash.deriveTransactionSeed(_, participantId, cmds.ledgerEffectiveTime)
              ),
            ) map { tx =>
              // Annotate the transaction with the package dependencies. Since
              // all commands are actions on a contract template, with a fully typed
              // argument, we only need to consider the templates mentioned in the command
              // to compute the full dependencies.
              val deps = processedCmds.foldLeft(Set.empty[PackageId]) {
                case (pkgIds, (_, cmd)) =>
                  val pkgId = cmd.templateId.packageId
                  val transitiveDeps =
                    _compiledPackages
                      .getPackageDependencies(pkgId)
                      .getOrElse(
                        sys.error(s"INTERNAL ERROR: Missing dependencies of package $pkgId"))
                  (pkgIds + pkgId) union transitiveDeps
              }
              tx.copy(optUsedPackages = Some(deps))
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
      submissionSeed: Option[crypto.Hash],
      participantId: Ref.ParticipantId,
      submitters: Set[Party],
      nodes: Seq[GenNode.WithTxValue[Value.NodeId, Value.ContractId]],
      ledgerEffectiveTime: Time.Timestamp
  ): Result[Transaction.Transaction] = {

    val transactionSeed = submissionSeed.map(
      crypto.Hash.deriveTransactionSeed(_, participantId, ledgerEffectiveTime)
    )

    val commandTranslation = new CommandPreprocessor(_compiledPackages)
    for {
      commands <- Result.sequence(ImmArray(nodes).map(translateNode(commandTranslation)))
      checkSubmitterInMaintainers <- ShouldCheckSubmitterInMaintainers(
        _compiledPackages,
        commands.map(_._2.templateId))
      // reinterpret is never used for submission, only for validation.
      result <- interpretCommands(
        validating = true,
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        submitters = submitters,
        commands = commands,
        time = ledgerEffectiveTime,
        transactionSeed = transactionSeed
      )
    } yield result
  }

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
      ledgerEffectiveTime: Time.Timestamp
  ): Result[Unit] = {
    import scalaz.std.option._
    import scalaz.syntax.traverse.ToTraverseOps
    val commandTranslation = new CommandPreprocessor(_compiledPackages)
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

      commands <- translateTransactionRoots(commandTranslation, tx)
      checkSubmitterInMaintainers <- ShouldCheckSubmitterInMaintainers(
        _compiledPackages,
        commands.map(_._2._2.templateId))
      rtx <- interpretCommands(
        transactionSeed = tx.transactionSeed,
        validating = true,
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        submitters = submitters,
        commands = commands.map(_._2),
        time = ledgerEffectiveTime
      )
      validationResult <- if (tx isReplayedBy rtx) {
        ResultDone(())
      } else {
        ResultError(
          ValidationError(
            s"recreated and original transaction mismatch $tx expected, but $rtx is recreated"))
      }
    } yield validationResult
  }

  // A safe cast of a value to a value which uses only absolute contract IDs.
  // In particular, the cast will succeed for all values contained in the root nodes of a Transaction produced by submit
  private[this] def asValueWithAbsoluteContractIds(
      v: Value[Value.ContractId]
  ): Result[Value[Value.AbsoluteContractId]] =
    v.ensureNoRelCid
      .fold(
        rcoid => ResultError(ValidationError(s"unexpected relative contract id $rcoid")),
        ResultDone(_)
      )

  private[this] def asAbsoluteContractId(coid: Value.ContractId): Result[Value.AbsoluteContractId] =
    coid match {
      case rcoid: Value.RelativeContractId =>
        ResultError(ValidationError(s"not an absolute contract ID: $rcoid"))
      case acoid: Value.AbsoluteContractId =>
        ResultDone(acoid)
    }

  // Translate a GenNode into an expression re-interpretable by the interpreter
  private[this] def translateNode[Cid <: Value.ContractId](
      commandPreprocessor: CommandPreprocessor)(
      node: GenNode.WithTxValue[Transaction.NodeId, Cid]): Result[(Type, SpeedyCommand)] = {

    node match {
      case NodeCreate(nodeSeed @ _, coid @ _, coinst, optLoc @ _, sigs @ _, stks @ _, key @ _) =>
        val identifier = coinst.template
        asValueWithAbsoluteContractIds(coinst.arg.value).flatMap(
          absArg => commandPreprocessor.preprocessCreate(identifier, absArg)
        )

      case NodeExercises(
          nodeSeed @ _,
          coid,
          template,
          choice,
          optLoc @ _,
          consuming @ _,
          actingParties @ _,
          chosenVal,
          stakeholders @ _,
          signatories @ _,
          controllers @ _,
          children @ _,
          exerciseResult @ _,
          key @ _) =>
        val templateId = template
        asValueWithAbsoluteContractIds(chosenVal.value).flatMap(
          absChosenVal =>
            commandPreprocessor
              .preprocessExercise(templateId, coid, choice, absChosenVal))

      case NodeFetch(coid, templateId, _, _, _, _) =>
        asAbsoluteContractId(coid)
          .flatMap(acoid => commandPreprocessor.preprocessFetch(templateId, acoid))

      case NodeLookupByKey(_, _, _, _) =>
        sys.error("TODO lookup by key command translate")
    }
  }

  private[this] def translateTransactionRoots[Cid <: Value.ContractId](
      commandPreprocessor: CommandPreprocessor,
      tx: GenTransaction.WithTxValue[Transaction.NodeId, Cid]
  ): Result[ImmArray[(Transaction.NodeId, (Type, SpeedyCommand))]] = {
    Result.sequence(tx.roots.map(id =>
      tx.nodes.get(id) match {
        case None =>
          ResultError(ValidationError(s"invalid transaction, root refers to non-existing node $id"))
        case Some(node) =>
          node match {
            case NodeFetch(_, _, _, _, _, _) =>
              ResultError(ValidationError(s"Transaction contains a fetch root node $id"))
            case _ =>
              translateNode(commandPreprocessor)(node).map((id, _)) match {
                case ResultError(ValidationError(msg)) =>
                  ResultError(ValidationError(s"Transaction node $id: $msg"))
                case x => x
              }
          }
    }))
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
      commands: ImmArray[(Type, SpeedyCommand)],
      time: Time.Timestamp,
      transactionSeed: Option[crypto.Hash]
  ): Result[Transaction.Transaction] = {
    val machine = Machine
      .build(
        checkSubmitterInMaintainers = checkSubmitterInMaintainers,
        sexpr = Compiler(compiledPackages.packages).compile(commands.map(_._2)),
        compiledPackages = _compiledPackages,
        seedWithTime = transactionSeed.map(_ -> time),
      )
      .copy(validating = validating, committers = submitters)
    interpretLoop(machine, time)
  }

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Return"))
  private[engine] def interpretLoop(
      machine: Machine,
      time: Time.Timestamp
  ): Result[Transaction.Transaction] = {
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

        case SResultMissingDefinition(ref, callback) =>
          return Result.needPackage(
            ref.packageId,
            pkg => {
              _compiledPackages.addPackage(ref.packageId, pkg).flatMap {
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
          callback(time)

        case SResultNeedKey(gk, _, cbMissing, cbPresent) =>
          return ResultNeedKey(
            gk, {
              case None =>
                if (!cbMissing(())) {
                  ResultError(Error(s"dependency error: couldn't find key $gk"))
                } else {
                  interpretLoop(machine, time)
                }
              case Some(key) =>
                cbPresent(key)
                interpretLoop(machine, time)
            }
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
      case Right(t) => ResultDone(t)
    }
  }

  def clearPackages(): Unit = _compiledPackages.clear()

  /** Note: it's important we return a [[com.digitalasset.daml.lf.CompiledPackages]],
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
