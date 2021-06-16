// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.crypto
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.lf.speedy.Speedy.{OffLedger, OnLedger}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SResult._
import com.daml.lf.value.Value

import scala.annotation.tailrec

private case class SRunnerException(err: SError) extends RuntimeException(err.toString)

/** Speedy scenario runner that uses the reference ledger.
  *
  * @constructor Creates a runner using an instance of [[Speedy.Machine]].
  * @param partyNameMangler allows to amend party names defined in scenarios,
  *        before they are executed against a ledger. The function should be idempotent
  *        in the context of a single {@code ScenarioRunner} life-time, i.e. return the
  *        same result each time given the same argument. Should return values compatible
  *        with [[com.daml.lf.data.Ref.Party]].
  */
final case class ScenarioRunner(
    machine: Speedy.Machine,
    initialSeed: crypto.Hash,
    partyNameMangler: (String => String) = identity,
) {
  import ScenarioRunner._

  var seed = initialSeed

  var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  val onLedger = machine.ledgerMode match {
    case OffLedger => throw SRequiresOnLedger("ScenarioRunner")
    case onLedger: OnLedger => onLedger
  }

  import scala.util.{Try, Success, Failure}

  def run(): Either[(SError, ScenarioLedger), (Double, Int, ScenarioLedger, SValue)] =
    handleUnsafe(runUnsafe()) match {
      case Left(err) => Left((err, ledger))
      case Right(t) => Right(t)
    }

  private def handleUnsafe[T](unsafe: => T): Either[SError, T] = {
    Try(unsafe) match {
      case Failure(SRunnerException(err)) => Left(err)
      case Failure(other) => throw other
      case Success(t) => Right(t)
    }
  }

  private[this] def nextSeed(submissionSeed: crypto.Hash): crypto.Hash =
    crypto.Hash.deriveTransactionSeed(
      submissionSeed,
      Ref.ParticipantId.assertFromString("scenario-service"),
      // MinValue makes no sense here but this is what we did before so
      // to avoid breaking all tests we keep it for now at least.
      Time.Timestamp.MinValue,
    )

  private def runUnsafe(): (Double, Int, ScenarioLedger, SValue) = {
    // NOTE(JM): Written with an imperative loop and exceptions for speed
    // and so that we don't need to worry about stack usage.
    val startTime = System.nanoTime()
    var steps = 0
    var finalValue: SValue = null
    while (finalValue == null) {
      //machine.print(steps)
      steps += 1 // this counts the number of external `Need` interactions
      val res: SResult = machine.run()
      res match {
        case SResultFinalValue(v) =>
          finalValue = v

        case SResultError(err) =>
          throw SRunnerException(err)

        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)

        case SResultScenarioPassTime(delta, callback) =>
          passTime(delta, callback)

        case SResultScenarioGetParty(partyText, callback) =>
          getParty(partyText, callback)

        case SResultScenarioSubmit(committers, commands, location, mustFail, callback) =>
          val submitResult = submit(committers, commands, location)
          // TODO (MK) We copy the ptx & commit location and the trace
          // log to the off-ledger machine as a temporary hack until
          // the callsites have changed sufficiently to allow us to
          // avoid this gross mess.
          machine.withOnLedger("runUnsafe") { onLedger =>
            onLedger.ptx = submitResult.ptx
            onLedger.commitLocation = location
          }
          submitResult.traceLog.iterator.foreach { case (msg, loc) =>
            machine.traceLog.add(msg, loc)
          }
          if (mustFail) {
            submitResult match {
              case Commit(result, _, _, _) =>
                throw new SRunnerException(
                  ScenarioErrorMustFailSucceeded(result.richTransaction.transaction)
                )
              case err: SubmissionError =>
                // TODO (MK) This is gross, we need to unwind the transaction to
                // get the right root context to derived the seed for the next transaction.
                val rootCtx = err.ptx.unwind.context
                seed = nextSeed(
                  rootCtx.nextActionChildSeed
                )
                ledger = ledger.insertAssertMustFail(committers, Set.empty, location)
                callback(SValue.SUnit)
            }
          } else {
            submitResult match {
              case Commit(result, value, _, _) =>
                seed = nextSeed(
                  crypto.Hash.deriveNodeSeed(seed, result.richTransaction.transaction.roots.length)
                )
                ledger = result.newLedger
                callback(value)
              case err: SubmissionError =>
                err match {
                  case CommitError(err, _, _) =>
                    throw new SRunnerException(ScenarioErrorCommitError(err))
                  case InterpretationError(err, _, _) =>
                    throw new SRunnerException(err)
                }
            }
          }

        case SResultNeedPackage(pkgId, _) =>
          crash(s"package $pkgId not found")

        case _: SResultNeedContract =>
          crash("SResultNeedContract outside of submission")

        case _: SResultNeedKey =>
          crash("SResultNeedKey outside of submission")

        case _: SResultNeedLocalKeyVisible =>
          crash("SResultNeedLocalKeyVisible outside of submission")
      }
    }
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    (diff, steps, ledger, finalValue)
  }

  private def submit(
      committers: Set[Party],
      commands: SValue,
      location: Option[Location],
  ): SubmissionResult = {
    val ledgerMachine = Speedy.Machine(
      compiledPackages = machine.compiledPackages,
      submissionTime = Time.Timestamp.MinValue,
      // TODO figure out what to do about the seed
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SExpr.SEApp(SExpr.SEValue(commands), Array(SExpr.SEValue(SValue.SToken))),
      globalCids = Set.empty,
      committers = committers,
    )
    val onLedger = ledgerMachine.ledgerMode match {
      case OffLedger => throw SRequiresOnLedger("ScenarioRunner")
      case onLedger: OnLedger => onLedger
    }
    @tailrec
    def go(): SubmissionResult = {
      ledgerMachine.run() match {
        case SResultFinalValue(resultValue) =>
          onLedger.ptxInternal.finish match {
            case PartialTransaction.CompleteTransaction(tx) =>
              ScenarioLedger.commitTransaction(
                actAs = committers,
                readAs = Set.empty,
                effectiveAt = ledger.currentTime,
                optLocation = location,
                tx = tx,
                l = ledger,
              ) match {
                case Left(fas) =>
                  CommitError(fas, onLedger.ptxInternal, ledgerMachine.traceLog)
                case Right(result) =>
                  Commit(result, resultValue, onLedger.ptxInternal, ledgerMachine.traceLog)
              }
            case PartialTransaction.IncompleteTransaction(ptx) =>
              throw new RuntimeException(s"Unexpected abort: $ptx")
          }
        case SResultError(err) =>
          InterpretationError(err, onLedger.ptxInternal, ledgerMachine.traceLog)
        case SResultNeedContract(coid, tid @ _, committers, cbMissing, cbPresent) =>
          lookupContract(coid, committers, Set.empty, cbMissing, cbPresent) match {
            case Left(err) => InterpretationError(err, onLedger.ptxInternal, ledgerMachine.traceLog)
            case Right(_) => go()
          }
        case SResultNeedKey(keyWithMaintainers, committers, cb) =>
          lookupKey(keyWithMaintainers.globalKey, committers, Set.empty, cb) match {
            case Left(err) => InterpretationError(err, onLedger.ptxInternal, ledgerMachine.traceLog)
            case Right(_) => go()
          }
        case SResultNeedLocalKeyVisible(stakeholders, committers, cb) =>
          val visible = SVisibleByKey.fromSubmitters(committers, Set.empty)(stakeholders)
          cb(visible)
          go()
        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)
          go()
        case SResultNeedPackage(pkgId, _) =>
          crash(s"package $pkgId not found")
        case _: SResultScenarioGetParty =>
          crash("SResultScenarioGetParty in submission")
        case _: SResultScenarioPassTime =>
          crash("SResultScenarioPassTime in submission")
        case _: SResultScenarioSubmit =>
          crash("SResultScenarioSubmit in submission")
      }
    }
    go()
  }

  private def crash(reason: String) =
    throw SRunnerException(SErrorCrash(reason))

  private def getParty(partyText: String, callback: Party => Unit) = {
    val mangledPartyText = partyNameMangler(partyText)
    Party.fromString(mangledPartyText) match {
      case Right(s) => callback(s)
      case Left(msg) => throw SRunnerException(ScenarioErrorInvalidPartyName(partyText, msg))
    }
  }

  private def passTime(delta: Long, callback: Time.Timestamp => Unit) = {
    ledger = ledger.passTime(delta)
    callback(ledger.currentTime)
  }

  private[lf] def lookupContract(
      acoid: ContractId,
      actAs: Set[Party],
      readAs: Set[Party],
      cbMissing: Unit => Boolean,
      cbPresent: ContractInst[Value.VersionedValue[ContractId]] => Unit,
  ): Either[SError, Unit] =
    handleUnsafe(lookupContractUnsafe(acoid, actAs, readAs, cbMissing, cbPresent))

  private def lookupContractUnsafe(
      acoid: ContractId,
      actAs: Set[Party],
      readAs: Set[Party],
      cbMissing: Unit => Boolean,
      cbPresent: ContractInst[Value.VersionedValue[ContractId]] => Unit,
  ) = {

    val effectiveAt = ledger.currentTime

    def missingWith(err: SError) =
      if (!cbMissing(()))
        throw SRunnerException(err)

    ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(actAs, readAs),
      effectiveAt = effectiveAt,
      acoid,
    ) match {
      case ScenarioLedger.LookupOk(_, coinst, _) =>
        cbPresent(coinst)

      case ScenarioLedger.LookupContractNotFound(coid) =>
        // This should never happen, hence we don't have a specific
        // error for this.
        missingWith(SErrorCrash(s"contract $coid not found"))

      case ScenarioLedger.LookupContractNotEffective(coid, tid, effectiveAt) =>
        missingWith(ScenarioErrorContractNotEffective(coid, tid, effectiveAt))

      case ScenarioLedger.LookupContractNotActive(coid, tid, consumedBy) =>
        missingWith(ScenarioErrorContractNotActive(coid, tid, consumedBy))

      case ScenarioLedger.LookupContractNotVisible(coid, tid, observers, stakeholders @ _) =>
        missingWith(ScenarioErrorContractNotVisible(coid, tid, actAs, readAs, observers))
    }
  }

  private[lf] def lookupKey(
      gk: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      canContinue: SKeyLookupResult => Boolean,
  ): Either[SError, Unit] =
    handleUnsafe(lookupKeyUnsafe(gk, actAs, readAs, canContinue))

  private def lookupKeyUnsafe(
      gk: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      canContinue: SKeyLookupResult => Boolean,
  ): Unit = {
    val effectiveAt = ledger.currentTime
    val readers = actAs union readAs

    def missingWith(err: SError) =
      if (!canContinue(SKeyLookupResult.NotFound))
        throw SRunnerException(err)

    def notVisibleWith(err: SError) =
      if (!canContinue(SKeyLookupResult.NotVisible))
        throw SRunnerException(err)

    ledger.ledgerData.activeKeys.get(gk) match {
      case None =>
        missingWith(DamlEContractKeyNotFound(gk))
      case Some(acoid) =>
        ledger.lookupGlobalContract(
          view = ScenarioLedger.ParticipantView(actAs, readAs),
          effectiveAt = effectiveAt,
          acoid,
        ) match {
          case ScenarioLedger.LookupOk(_, _, stakeholders) =>
            if (!readers.intersect(stakeholders).isEmpty)
              // We should always be able to continue with a SKeyLookupResult.Found.
              // Run to get side effects and assert result.
              assert(canContinue(SKeyLookupResult.Found(acoid)))
            else
              notVisibleWith(
                ScenarioErrorContractKeyNotVisible(acoid, gk, actAs, readAs, stakeholders)
              )
          case ScenarioLedger.LookupContractNotFound(coid) =>
            missingWith(SErrorCrash(s"contract $coid not found, but we found its key!"))
          case ScenarioLedger.LookupContractNotEffective(_, _, _) =>
            missingWith(SErrorCrash(s"contract $acoid not effective, but we found its key!"))
          case ScenarioLedger.LookupContractNotActive(_, _, _) =>
            missingWith(SErrorCrash(s"contract $acoid not active, but we found its key!"))
          case ScenarioLedger.LookupContractNotVisible(
                coid,
                tid @ _,
                observers @ _,
                stakeholders,
              ) =>
            notVisibleWith(
              ScenarioErrorContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders)
            )
        }
    }
  }
}

object ScenarioRunner {

  @deprecated("can be used only by sandbox classic.", since = "1.4.0")
  def getScenarioLedger(
      engine: Engine,
      scenarioRef: Ref.DefinitionRef,
      scenarioDef: Ast.Definition,
      transactionSeed: crypto.Hash,
  ): ScenarioLedger = {
    val scenarioExpr = getScenarioExpr(scenarioRef, scenarioDef)
    val speedyMachine = Speedy.Machine.fromScenarioExpr(
      engine.compiledPackages(),
      transactionSeed,
      scenarioExpr,
    )
    ScenarioRunner(speedyMachine, transactionSeed).run() match {
      case Left(e) =>
        throw new RuntimeException(s"error running scenario $scenarioRef in scenario $e")
      case Right((_, _, l, _)) => l
    }
  }

  private[this] def getScenarioExpr(
      scenarioRef: Ref.DefinitionRef,
      scenarioDef: Ast.Definition,
  ): Ast.Expr = {
    scenarioDef match {
      case Ast.DValue(_, _, body, _) => body
      case _: Ast.DTypeSyn =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a type synonym, not a definition"
        )
      case _: Ast.DDataType =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a data type, not a definition"
        )
    }
  }

  sealed trait SubmissionResult {
    // TODO (MK) Temporary to leak the ptx from the submission machine
    // to the parent machine.
    def ptx: PartialTransaction
    def traceLog: TraceLog
  }

  final case class Commit(
      result: ScenarioLedger.CommitResult,
      value: SValue,
      ptx: PartialTransaction,
      traceLog: TraceLog,
  ) extends SubmissionResult

  sealed trait SubmissionError extends SubmissionResult {}
  final case class CommitError(
      error: ScenarioLedger.CommitError,
      ptx: PartialTransaction,
      traceLog: TraceLog,
  ) extends SubmissionError
  final case class InterpretationError(error: SError, ptx: PartialTransaction, traceLog: TraceLog)
      extends SubmissionError
}
