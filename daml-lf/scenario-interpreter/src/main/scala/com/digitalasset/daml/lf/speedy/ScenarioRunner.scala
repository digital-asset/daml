// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.CompiledPackages
import com.daml.lf.crypto
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{GlobalKey, SubmittedTransaction}
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.lf.speedy.Speedy.{OffLedger, OnLedger}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.IncompleteTransaction
import com.daml.lf.value.Value

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

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
  var currentSubmission: Option[CurrentSubmission] = None

  def run(): ScenarioResult =
    handleUnsafe(runUnsafe()) match {
      case Left(err) =>
        ScenarioError(ledger, machine.traceLog, currentSubmission, machine.stackTrace(), err)
      case Right(t) => t
    }

  private def runUnsafe(): ScenarioSuccess = {
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
          val submitResult = submit(
            machine.compiledPackages,
            ScenarioLedgerApi(ledger),
            committers,
            Set.empty,
            SExpr.SEValue(commands),
            location,
            seed,
            machine.traceLog,
          )
          if (mustFail) {
            submitResult match {
              case Commit(result, _, ptx) =>
                currentSubmission = Some(CurrentSubmission(location, ptx.finishIncomplete))
                throw new SRunnerException(
                  ScenarioErrorMustFailSucceeded(result.richTransaction.transaction)
                )
              case err: SubmissionError =>
                currentSubmission = None
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
              case Commit(result, value, _) =>
                currentSubmission = None
                seed = nextSeed(
                  crypto.Hash.deriveNodeSeed(seed, result.richTransaction.transaction.roots.length)
                )
                ledger = result.newLedger
                callback(value)
              case SubmissionError(err, ptx) =>
                currentSubmission = Some(CurrentSubmission(location, ptx.finishIncomplete))
                throw new SRunnerException(err)
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
    ScenarioSuccess(ledger, machine.traceLog, diff, steps, finalValue)
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
      scenarioExpr,
    )
    ScenarioRunner(speedyMachine, transactionSeed).run() match {
      case err: ScenarioError =>
        throw new RuntimeException(s"error running scenario $scenarioRef in scenario ${err.error}")
      case success: ScenarioSuccess => success.ledger
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

  private def handleUnsafe[T](unsafe: => T): Either[SError, T] = {
    Try(unsafe) match {
      case Failure(SRunnerException(err)) => Left(err)
      case Failure(other) => throw other
      case Success(t) => Right(t)
    }
  }

  sealed trait SubmissionResult[+R] {
    // TODO (MK) Temporary to leak the ptx from the submission machine
    // to the parent machine.
    def ptx: PartialTransaction
  }

  final case class Commit[R](
      result: R,
      value: SValue,
      ptx: PartialTransaction,
  ) extends SubmissionResult[R]

  final case class SubmissionError(error: SError, ptx: PartialTransaction)
      extends SubmissionResult[Nothing]

  // The interface we need from a ledger during submission. We allow abstracting over this so we can play
  // tricks like caching all responses in some benchmarks.
  abstract class LedgerApi[R] {
    def lookupContract(
        coid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        cbPresent: ContractInst[Value.VersionedValue[ContractId]] => Unit,
    ): Either[SError, Unit]
    def lookupKey(
        machine: Speedy.Machine,
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        canContinue: Option[ContractId] => Boolean,
    ): Either[SError, Unit]
    def currentTime: Time.Timestamp
    def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: SubmittedTransaction,
    ): Either[SError, R]
  }

  case class ScenarioLedgerApi(ledger: ScenarioLedger)
      extends LedgerApi[ScenarioLedger.CommitResult] {

    override def lookupContract(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: ContractInst[Value.VersionedValue[ContractId]] => Unit,
    ): Either[SError, Unit] =
      handleUnsafe(lookupContractUnsafe(acoid, actAs, readAs, callback))

    private def lookupContractUnsafe(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: ContractInst[Value.VersionedValue[ContractId]] => Unit,
    ) = {

      val effectiveAt = ledger.currentTime

      def missingWith(err: SError) = throw SRunnerException(err)

      ledger.lookupGlobalContract(
        view = ScenarioLedger.ParticipantView(actAs, readAs),
        effectiveAt = effectiveAt,
        acoid,
      ) match {
        case ScenarioLedger.LookupOk(_, coinst, _) =>
          callback(coinst)

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

    override def lookupKey(
        machine: Speedy.Machine,
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Either[SError, Unit] =
      handleUnsafe(lookupKeyUnsafe(machine: Speedy.Machine, gk, actAs, readAs, callback))

    private def lookupKeyUnsafe(
        machine: Speedy.Machine,
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Unit = {

      val effectiveAt = ledger.currentTime
      val readers = actAs union readAs

      def missingWith(err: SError) =
        if (!callback(None)) {
          machine.returnValue = null
          machine.ctrl = null
          throw SRunnerException(err)
        }

      ledger.ledgerData.activeKeys.get(gk) match {
        case None =>
          missingWith(SErrorDamlException(interpretation.Error.ContractKeyNotFound(gk)))
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
                assert(callback(Some(acoid)))
              else
                throw SRunnerException(
                  ScenarioErrorContractKeyNotVisible(acoid, gk, actAs, readAs, stakeholders)
                )
            case ScenarioLedger.LookupContractNotFound(coid) =>
              missingWith(SErrorCrash(s"contract ${coid.coid} not found, but we found its key!"))
            case ScenarioLedger.LookupContractNotEffective(_, _, _) =>
              missingWith(
                SErrorCrash(s"contract ${acoid.coid} not effective, but we found its key!")
              )
            case ScenarioLedger.LookupContractNotActive(_, _, _) =>
              missingWith(SErrorCrash(s"contract ${acoid.coid} not active, but we found its key!"))
            case ScenarioLedger.LookupContractNotVisible(
                  coid,
                  tid @ _,
                  observers @ _,
                  stakeholders,
                ) =>
              throw SRunnerException(
                ScenarioErrorContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders)
              )
          }
      }
    }

    override def currentTime = ledger.currentTime
    override def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: SubmittedTransaction,
    ): Either[SError, ScenarioLedger.CommitResult] =
      ScenarioLedger.commitTransaction(
        actAs = committers,
        readAs = readAs,
        effectiveAt = ledger.currentTime,
        optLocation = location,
        tx = tx,
        l = ledger,
      ) match {
        case Left(fas) =>
          Left(ScenarioErrorCommitError(fas))
        case Right(result) =>
          Right(result)
      }
  }

  def submit[R](
      compiledPackages: CompiledPackages,
      ledger: LedgerApi[R],
      committers: Set[Party],
      readAs: Set[Party],
      commands: SExpr,
      location: Option[Location],
      seed: crypto.Hash,
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
  ): SubmissionResult[R] = {
    val ledgerMachine = Speedy.Machine(
      compiledPackages = compiledPackages,
      submissionTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SExpr.SEApp(commands, Array(SExpr.SEValue(SValue.SToken))),
      globalCids = Set.empty,
      committers = committers,
      traceLog = traceLog,
    )
    val onLedger = ledgerMachine.ledgerMode match {
      case OffLedger => throw SRequiresOnLedger("ScenarioRunner")
      case onLedger: OnLedger => onLedger
    }
    @tailrec
    def go(): SubmissionResult[R] = {
      ledgerMachine.run() match {
        case SResultFinalValue(resultValue) =>
          onLedger.ptxInternal.finish match {
            case PartialTransaction.CompleteTransaction(tx, _) =>
              ledger.commit(committers, readAs, location, tx) match {
                case Left(err) => SubmissionError(err, onLedger.ptxInternal)
                case Right(r) =>
                  Commit(r, resultValue, onLedger.ptxInternal)
              }
            case PartialTransaction.IncompleteTransaction(ptx) =>
              throw new RuntimeException(s"Unexpected abort: $ptx")
          }
        case SResultError(err) =>
          SubmissionError(err, onLedger.ptxInternal)
        case SResultNeedContract(coid, tid @ _, committers, callback) =>
          ledger.lookupContract(coid, committers, readAs, callback) match {
            case Left(err) => SubmissionError(err, onLedger.ptxInternal)
            case Right(_) => go()
          }
        case SResultNeedKey(keyWithMaintainers, committers, callback) =>
          ledger.lookupKey(
            ledgerMachine,
            keyWithMaintainers.globalKey,
            committers,
            readAs,
            callback,
          ) match {
            case Left(err) => SubmissionError(err, onLedger.ptxInternal)
            case Right(_) => go()
          }
        case SResultNeedLocalKeyVisible(stakeholders, committers, cb) =>
          val visible = SVisibleByKey.fromSubmitters(committers, readAs)(stakeholders)
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

  private[lf] def nextSeed(submissionSeed: crypto.Hash): crypto.Hash =
    crypto.Hash.deriveTransactionSeed(
      submissionSeed,
      Ref.ParticipantId.assertFromString("scenario-service"),
      // MinValue makes no sense here but this is what we did before so
      // to avoid breaking all tests we keep it for now at least.
      Time.Timestamp.MinValue,
    )

  sealed abstract class ScenarioResult extends Product with Serializable {
    def ledger: ScenarioLedger
    def traceLog: TraceLog
  }

  final case class CurrentSubmission(
      commitLocation: Option[Location],
      ptx: IncompleteTransaction,
  )

  final case class ScenarioSuccess(
      ledger: ScenarioLedger,
      traceLog: TraceLog,
      duration: Double,
      steps: Int,
      resultValue: SValue,
  ) extends ScenarioResult
  final case class ScenarioError(
      ledger: ScenarioLedger,
      traceLog: TraceLog,
      currentSubmission: Option[CurrentSubmission],
      stackTrace: ImmArray[Location],
      error: SError,
  ) extends ScenarioResult
}
