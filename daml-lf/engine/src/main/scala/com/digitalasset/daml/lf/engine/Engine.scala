// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref.{Party, SimpleString}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.Pretty
import com.digitalasset.daml.lf.speedy.Speedy.Machine
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{Transaction => Tx}
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.speedy.{Command => SpeedyCommand}

import scala.annotation.tailrec

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
  private[this] val compiledPackages = ConcurrentCompiledPackages()
  private[this] val commandTranslation = CommandPreprocessor(compiledPackages)

  /**
    * Executes commands `cmds` and returns one of the following:
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
    */
  def submit(cmds: Commands): Result[Transaction.Transaction] = {
    commandTranslation
      .preprocessCommands(cmds)
      .flatMap(interpret(_, cmds.ledgerEffectiveTime))
  }

  /**
    * Behaves like `submit`, but it takes GenNode arguments instead of a Commands argument.
    * That is, it can be used to reinterpret an already interpreted transaction (since it consists of GenNodes).
    * Formally, the following is guaranteed to hold for all pcs, pkgs, and keys, when evaluated on the same Engine:
    * evaluate(submit(cmds)) = ResultDone(tx) ==> evaluate(reinterpret(txRoots, cmds.ledgerEffectiveTime)) === ResultDone(tx)
    * where:
    *   evaluate(result) = result.consume(pcs, pkgs, keys)
    *   txRoots = tx.roots.map(id => tx.nodes.get(id).get).toSeq
    *   tx === tx' if tx and tx' are equivalent modulo a renaming of node and relative contract IDs
    *
    * In addition to the errors returned by `submit`, reinterpretation fails with a `ValidationError` whenever `nodes`
    * contain a relative contract ID, either as the target contract of a fetch, or as an argument to a
    * create or an exercise choice.
    */
  def reinterpret(
      nodes: Seq[GenNode.WithTxValue[NodeId, ContractId]],
      ledgerEffectiveTime: Time.Timestamp
  ): Result[Transaction.Transaction] = {
    for {
      commands <- Result.sequence(ImmArray(nodes).map(translateNode(commandTranslation)))
      result <- interpret(commands, ledgerEffectiveTime)
    } yield result
  }

  /**
    * given a full transaction check if it can be a valid result of a command. this is supposed to run _before_ commit,
    * and in fact takes the uncommitted transaction which still has the relative contract ids.
    *
    * this function will return an error if relative contract ids are mentioned in the root nodes, which is OK since commands
    * cannot contain relative contract ids, and root nodes come from commands, and you are only supposed to use this
    * function for transactions coming from a list of commands.
    *
    *  @param tx a complete unblinded Transaction to be validated
    *  @param ledgerEffectiveTime time when the transaction is claimed to be submitted
    */
  def validate(
      tx: Transaction.Transaction,
      ledgerEffectiveTime: Time.Timestamp
  ): Result[Unit] = {
    //reinterpret
    for {
      commands <- translateTransactionRoots(commandTranslation, tx)
      rtx <- interpret(commands.map(_._2), ledgerEffectiveTime)
      validationResult <- if (tx isReplayedBy rtx) {
        ResultDone(())
      } else {
        ResultError(
          ValidationError(
            s"recreated and original transaction mismatch $tx expected, but $rtx is recreated"))
      }
    } yield validationResult
  }

  /**
    * Post-commit validation
    * we demand that validatable transactions only contain AbsoluteContractIds in root nodes
    *
    * @param tx a transaction to be validated
    * @param submitter party name if known who originally submitted the transaction
    * @param ledgerEffectiveTime time of the original submission
    * @param requestor the name of the party requesting this validation
    * @param contractIdMaping a function that generates absolute contractIds
    */
  def validatePartial(
      tx: GenTransaction.WithTxValue[Tx.NodeId, AbsoluteContractId],
      submitter: Option[SimpleString],
      ledgerEffectiveTime: Time.Timestamp,
      requestor: Party,
      contractIdMaping: ContractId => AbsoluteContractId,
      valMapping: Tx.Value[ContractId] => Tx.Value[AbsoluteContractId]): Result[Unit] = {

    // we run the interpreter incrementally on root expressions,
    // so that we get the node indexing matching
    def incrementalRunInterpreter(): Result[Transaction.Transaction] = {
      @tailrec
      def go(
          state: Result[Transaction.Transaction],
          roots: FrontStack[(Transaction.NodeId, (Type, SpeedyCommand))])
        : Result[Transaction.Transaction] = {
        roots match {
          case FrontStack() => state
          case FrontStackCons((id, (_, cmd)), rs) =>
            val nextStep: Result[Transaction.Transaction] = for {
              t <- interpretFromNodeId(cmd, id, ledgerEffectiveTime)
              o <- state
              newNodes = t.nodes ++ o.nodes
            } yield o.copy(nodes = newNodes, roots = (BackStack(o.roots) :++ t.roots).toImmArray)
            go(nextStep, rs)
        }
      }

      val comps =
        translateTransactionRoots(commandTranslation, tx)
          .flatMap(
            s =>
              if (s.isEmpty)
                ResultError(Error("transaction with empty roots cannot be validated"))
              else ResultDone(s))

      val firstRootNodeExpr = comps.map(_.head)
      val restRootExpressions = comps.map(_.tail)
      val init =
        firstRootNodeExpr.flatMap(p => interpretFromNodeId(p._2._2, p._1, ledgerEffectiveTime))

      restRootExpressions.flatMap(nodeExpressions => go(init, FrontStack(nodeExpressions)))
    }

    val checkedFailures: ((Transaction.NodeId, Ledger.FailedAuthorization)) => Boolean = {
      case (nid, failure) =>
        failure match {
          case Ledger.FACreateMissingAuthorization(_, _, _, requiredParties) =>
            tx.roots.toSeq.contains(nid) && requiredParties.contains(requestor)
          case Ledger.FAExerciseMissingAuthorization(_, _, _, _, requiredParties) =>
            tx.roots.toSeq.contains(nid) && requiredParties.contains(requestor)
          case _ => true
        }
    }

    // since partial transaction interpretation cannot always recover location
    // when some parent nodes are hidden and interpretation is initialized with None
    // but the input has non-empty location, so when not given both locations we won't compare
    // this is not part of the generic utilities because it only valid in this context
    def nodeEqualityWithoutLocation(
        n1: GenNode.WithTxValue[Tx.NodeId, AbsoluteContractId],
        n2: GenNode.WithTxValue[Tx.NodeId, AbsoluteContractId]) = {

      def removeLocation(n: GenNode.WithTxValue[Tx.NodeId, AbsoluteContractId])
        : GenNode.WithTxValue[Tx.NodeId, AbsoluteContractId] = {
        n match {
          case c: NodeCreate[_, _] => c.copy(optLocation = None)
          case e: NodeExercises[_, _, _] => e.copy(optLocation = None)
          case f: NodeFetch[_] => f.copy(optLocation = None)
          case l: NodeLookupByKey[_, _] => l.copy(optLocation = None)
        }
      }

      removeLocation(n1) == removeLocation(n2)

    }

    for {
      recreatedTx <- incrementalRunInterpreter()
      authorizerSet = submitter.map(s => Set(s)).getOrElse(Set.empty[Party])
      enrichment = Ledger.enrichTransaction(Ledger.Authorize(authorizerSet), recreatedTx)
      comparableTx = recreatedTx.mapContractIdAndValue(contractIdMaping, valMapping)
      _ <- Result.assert(!enrichment.failedAuthorizations.exists(checkedFailures))(
        Error("Post-commit validation failure: unauthorized transaction"))
      _ <- Result.assert(comparableTx.roots == tx.roots)(Error(
        s"Post-commit validation failure: transaction roots are in disagreement ${comparableTx.roots}, ${tx.roots}"))
      _ <- Result.assert(comparableTx.compareForest(tx)(nodeEqualityWithoutLocation))(Error(
        s"Post-commit validation failure: transaction nodes are in disagreement ${comparableTx} , ${tx}"))
    } yield ()

  }

  // A safe cast of a value to a value which uses only absolute contract IDs.
  // In particular, the cast will succeed for all values contained in the root nodes of a Transaction produced by submit
  private[this] def asValueWithAbsoluteContractIds[Cid](
      v: VersionedValue[Cid]): Result[VersionedValue[AbsoluteContractId]] =
    try {
      ResultDone(
        v.mapContractId {
          case rcoid: RelativeContractId =>
            throw ValidationError(s"unexpected relative contract id $rcoid")
          case acoid: AbsoluteContractId => acoid
        }
      )
    } catch {
      case err: ValidationError => ResultError(err)
    }

  private[this] def asAbsoluteContractId(coid: ContractId): Result[AbsoluteContractId] =
    coid match {
      case rcoid: RelativeContractId =>
        ResultError(ValidationError(s"not an absolute contract ID: $rcoid"))
      case acoid: AbsoluteContractId =>
        ResultDone(acoid)
    }

  // Translate a GenNode into an expression re-interpretable by the interpreter
  private[this] def translateNode[Cid <: ContractId](commandPreprocessor: CommandPreprocessor)(
      node: GenNode.WithTxValue[Transaction.NodeId, Cid]): Result[(Type, SpeedyCommand)] = {

    node match {
      case NodeCreate(coid @ _, coinst, optLoc @ _, sigs @ _, stks @ _, key @ _) =>
        val identifier = coinst.template
        asValueWithAbsoluteContractIds(coinst.arg).flatMap(
          absArg => commandPreprocessor.preprocessCreate(identifier, absArg)
        )

      case NodeExercises(
          coid,
          template,
          choice,
          optLoc @ _,
          consuming @ _,
          actingParties,
          chosenVal,
          stakeholders @ _,
          signatories @ _,
          controllers @ _,
          children @ _) =>
        val templateId = template
        asValueWithAbsoluteContractIds(chosenVal).flatMap(
          absChosenVal =>
            commandPreprocessor
              .preprocessExercise(templateId, coid, choice, actingParties, absChosenVal))

      case NodeFetch(coid, templateId, _, _, _, _) =>
        asAbsoluteContractId(coid)
          .flatMap(acoid => commandPreprocessor.preprocessFetch(templateId, acoid))

      case NodeLookupByKey(_, _, _, _) =>
        sys.error("TODO lookup by key command translate")
    }
  }

  private[this] def translateTransactionRoots[Cid <: ContractId](
      commandPreprocessor: CommandPreprocessor,
      tx: GenTransaction.WithTxValue[Transaction.NodeId, Cid]
  ): Result[ImmArray[(Transaction.NodeId, (Type, SpeedyCommand))]] = {
    Result.sequence(tx.roots.map(id =>
      tx.nodes.get(id) match {
        case None =>
          ResultError(ValidationError(s"invalid transaction, root refers to non-existing node $id"))
        case Some(node) =>
          translateNode(commandPreprocessor)(node).map((id, _)) match {
            case ResultError(ValidationError(msg)) =>
              ResultError(ValidationError(s"Transaction node $id: $msg"))
            case x => x
          }
    }))
  }

  private[engine] def interpretFromNodeId(
      command: SpeedyCommand,
      nodeId: Transaction.NodeId,
      time: Time.Timestamp
  ): Result[Transaction.Transaction] = {

    val machine =
      Machine.build(Compiler(compiledPackages.packages).compile(command), compiledPackages)
    machine.ptx = machine.ptx.copy(nextNodeId = nodeId)
    interpretLoop(machine, time)
  }

  private[engine] def interpret(
      expr: Expr,
      time: Time.Timestamp): Result[Transaction.Transaction] = {
    val machine =
      Machine.build(Compiler(compiledPackages.packages).compile(expr), compiledPackages)

    interpretLoop(machine, time)
  }

  private[engine] def interpret(
      commands: ImmArray[(Type, SpeedyCommand)],
      time: Time.Timestamp): Result[Transaction.Transaction] = {
    val machine = Machine.build(
      Compiler(compiledPackages.packages).compile(commands.map(_._2)),
      compiledPackages)

    interpretLoop(machine, time)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[engine] def interpretLoop(
      machine: Machine,
      time: Time.Timestamp
  ): Result[Transaction.Transaction] = {
    while (!machine.isFinal) {
      machine.step match {
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
              compiledPackages.addPackage(ref.packageId, pkg).flatMap {
                case _ =>
                  callback(compiledPackages)
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

        case SResultNeedKey(gk, cbMissing, cbPresent) =>
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

  def clearPackages(): Unit = compiledPackages.clear()
}

object Engine {
  def apply(): Engine = new Engine()
}
