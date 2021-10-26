// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, ValueEnricher, Result, ResultDone, ResultError}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.{GlobalKey, NodeId, SubmittedTransaction}
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.lf.speedy._
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.IncompleteTransaction
import com.daml.lf.value.Value
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** Speedy scenario runner that uses the reference ledger.
  *
  * @constructor Creates a runner using an instance of [[Speedy.Machine]].
  * @param partyNameMangler allows to amend party names defined in scenarios,
  *        before they are executed against a ledger. The function should be idempotent
  *        in the context of a single {@code ScenarioRunner} life-time, i.e. return the
  *        same result each time given the same argument. Should return values compatible
  *        with [[com.daml.lf.data.Ref.Party]].
  */
final class ScenarioRunner(
    machine: Speedy.Machine,
    initialSeed: crypto.Hash,
    partyNameMangler: (String => String) = identity,
) {
  import ScenarioRunner._

  private[this] val nextSeed: () => crypto.Hash = crypto.Hash.secureRandom(initialSeed)

  var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  var currentSubmission: Option[CurrentSubmission] = None

  def run(): ScenarioResult =
    handleUnsafe(runUnsafe()) match {
      case Left(err) =>
        ScenarioError(
          ledger,
          machine.traceLog,
          machine.warningLog,
          currentSubmission,
          machine.stackTrace(),
          err,
        )
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
          throw scenario.Error.RunnerException(err)

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
            nextSeed(),
            machine.traceLog,
            machine.warningLog,
          )
          if (mustFail) {
            submitResult match {
              case Commit(result, _, tx) =>
                currentSubmission = Some(CurrentSubmission(location, tx))
                throw scenario.Error.MustFailSucceeded(result.richTransaction.transaction)
              case _: SubmissionError =>
                ledger = ledger.insertAssertMustFail(committers, Set.empty, location)
                callback(SValue.SUnit)
            }
          } else {
            submitResult match {
              case Commit(result, value, _) =>
                currentSubmission = None
                ledger = result.newLedger
                callback(value)
              case SubmissionError(err, tx) =>
                currentSubmission = Some(CurrentSubmission(location, tx))
                throw err
            }
          }

        case SResultNeedPackage(pkgId, context, _) =>
          crash(LookupError.MissingPackage.pretty(pkgId, context))

        case _: SResultNeedContract =>
          crash("SResultNeedContract outside of submission")

        case _: SResultNeedKey =>
          crash("SResultNeedKey outside of submission")
      }
    }
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    ScenarioSuccess(ledger, machine.traceLog, machine.warningLog, diff, steps, finalValue)
  }

  private def getParty(partyText: String, callback: Party => Unit) = {
    val mangledPartyText = partyNameMangler(partyText)
    Party.fromString(mangledPartyText) match {
      case Right(s) => callback(s)
      case Left(msg) => throw Error.InvalidPartyName(partyText, msg)
    }
  }

  private def passTime(delta: Long, callback: Time.Timestamp => Unit) = {
    ledger = ledger.passTime(delta)
    callback(ledger.currentTime)
  }
}

object ScenarioRunner {

  private def crash(reason: String) =
    throw Error.Internal(reason)

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
    new ScenarioRunner(speedyMachine, transactionSeed).run() match {
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

  private def handleUnsafe[T](unsafe: => T): Either[Error, T] = {
    Try(unsafe) match {
      case Failure(err: Error) => Left(err: Error)
      case Failure(other) => throw other
      case Success(t) => Right(t)
    }
  }

  sealed trait SubmissionResult[+R]

  final case class Commit[R](
      result: R,
      value: SValue,
      tx: IncompleteTransaction,
  ) extends SubmissionResult[R]

  final case class SubmissionError(error: Error, tx: IncompleteTransaction)
      extends SubmissionResult[Nothing]

  // The interface we need from a ledger during submission. We allow abstracting over this so we can play
  // tricks like caching all responses in some benchmarks.
  abstract class LedgerApi[R] {
    def lookupContract(
        coid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        cbPresent: VersionedContractInstance => Unit,
    ): Either[Error, Unit]
    def lookupKey(
        machine: Speedy.Machine,
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        canContinue: Option[ContractId] => Boolean,
    ): Either[Error, Unit]
    def currentTime: Time.Timestamp
    def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: SubmittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, R]
  }

  case class ScenarioLedgerApi(ledger: ScenarioLedger)
      extends LedgerApi[ScenarioLedger.CommitResult] {

    override def lookupContract(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: VersionedContractInstance => Unit,
    ): Either[Error, Unit] =
      handleUnsafe(lookupContractUnsafe(acoid, actAs, readAs, callback))

    private def lookupContractUnsafe(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: VersionedContractInstance => Unit,
    ) = {

      val effectiveAt = ledger.currentTime

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
          throw Error.Internal(s"contract ${coid.coid} not found")

        case ScenarioLedger.LookupContractNotEffective(coid, tid, effectiveAt) =>
          throw Error.ContractNotEffective(coid, tid, effectiveAt)

        case ScenarioLedger.LookupContractNotActive(coid, tid, consumedBy) =>
          throw Error.ContractNotActive(coid, tid, consumedBy)

        case ScenarioLedger.LookupContractNotVisible(coid, tid, observers, stakeholders @ _) =>
          throw Error.ContractNotVisible(coid, tid, actAs, readAs, observers)
      }
    }

    override def lookupKey(
        machine: Speedy.Machine,
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Either[Error, Unit] =
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

      def missingWith(err: Error) =
        if (!callback(None)) {
          machine.returnValue = null
          machine.ctrl = null
          throw err
        }

      ledger.ledgerData.activeKeys.get(gk) match {
        case None =>
          missingWith(
            Error.RunnerException(
              SError.SErrorDamlException(interpretation.Error.ContractKeyNotFound(gk))
            )
          )
        case Some(acoid) =>
          ledger.lookupGlobalContract(
            view = ScenarioLedger.ParticipantView(actAs, readAs),
            effectiveAt = effectiveAt,
            acoid,
          ) match {
            case ScenarioLedger.LookupOk(_, _, stakeholders) =>
              if (!readers.intersect(stakeholders).isEmpty)
                // Note that even with a successful global lookup
                // the callback can return false. This happens for a fetch-by-key
                // if the contract got archived in the meantime.
                // We discard the result here and rely on fetch-by-key
                // setting up the state such that continuing interpretation fails.
                discard(callback(Some(acoid)))
              else
                throw Error.ContractKeyNotVisible(acoid, gk, actAs, readAs, stakeholders)
            case ScenarioLedger.LookupContractNotFound(coid) =>
              missingWith(
                Error.Internal(s"contract ${coid.coid} not found, but we found its key!")
              )
            case ScenarioLedger.LookupContractNotEffective(_, _, _) =>
              missingWith(
                Error.Internal(
                  s"contract ${acoid.coid} not effective, but we found its key!"
                )
              )
            case ScenarioLedger.LookupContractNotActive(_, _, _) =>
              missingWith(
                Error.Internal(s"contract ${acoid.coid} not active, but we found its key!")
              )
            case ScenarioLedger.LookupContractNotVisible(
                  coid,
                  tid @ _,
                  observers @ _,
                  stakeholders,
                ) =>
              throw Error.ContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders)
          }
      }
    }

    override def currentTime = ledger.currentTime

    override def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: SubmittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, ScenarioLedger.CommitResult] =
      ScenarioLedger.commitTransaction(
        actAs = committers,
        readAs = readAs,
        effectiveAt = ledger.currentTime,
        optLocation = location,
        tx = tx,
        locationInfo = locationInfo,
        l = ledger,
      ) match {
        case Left(fas) =>
          Left(Error.CommitError(fas))
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
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      doEnrichment: Boolean = true,
  ): SubmissionResult[R] = {
    val ledgerMachine = Speedy.Machine(
      compiledPackages = compiledPackages,
      submissionTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SExpr.SEApp(commands, Array(SExpr.SEValue(SValue.SToken))),
      committers = committers,
      readAs = readAs,
      traceLog = traceLog,
      warningLog = warningLog,
      commitLocation = location,
    )
    val onLedger = ledgerMachine.withOnLedger(NameOf.qualifiedNameOfCurrentFunc)(identity)

    def enrich(tx: SubmittedTransaction): SubmittedTransaction = {
      val config = Engine.DevEngine().config
      val valueTranslator =
        new ValueTranslator(
          interface = compiledPackages.interface,
          forbidV0ContractId = config.forbidV0ContractId,
          requireV1ContractIdSuffix = config.requireSuffixedGlobalContractId,
        )
      def translateValue(typ: Ast.Type, value: Value): Result[SValue] =
        valueTranslator.translateValue(typ, value) match {
          case Left(err) => ResultError(err)
          case Right(sv) => ResultDone(sv)
        }
      def loadPackage(pkgId: PackageId, context: language.Reference): Result[Unit] = {
        crash(LookupError.MissingPackage.pretty(pkgId, context))
      }
      val enricher = new ValueEnricher(compiledPackages, translateValue, loadPackage)
      def consume[V](res: Result[V]): V =
        res match {
          case ResultDone(x) => x
          case x => crash(s"unexpected Result when enriching value: $x")
        }
      SubmittedTransaction(consume(enricher.enrichTransaction(tx)))
    }

    @tailrec
    def go(): SubmissionResult[R] = {
      ledgerMachine.run() match {
        case SResult.SResultFinalValue(resultValue) =>
          onLedger.ptxInternal.finish match {
            case PartialTransaction.CompleteTransaction(tx0, locationInfo, _) =>
              val tx = if (doEnrichment) enrich(tx0) else tx0
              ledger.commit(committers, readAs, location, tx, locationInfo) match {
                case Left(err) =>
                  SubmissionError(err, onLedger.incompleteTransaction)
                case Right(r) =>
                  Commit(r, resultValue, onLedger.incompleteTransaction)
              }
            case PartialTransaction.IncompleteTransaction(ptx) =>
              throw new RuntimeException(s"Unexpected abort: $ptx")
          }
        case SResultError(err) =>
          SubmissionError(Error.RunnerException(err), onLedger.incompleteTransaction)
        case SResultNeedContract(coid, tid @ _, committers, callback) =>
          ledger.lookupContract(coid, committers, readAs, callback) match {
            case Left(err) => SubmissionError(err, onLedger.incompleteTransaction)
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
            case Left(err) => SubmissionError(err, onLedger.incompleteTransaction)
            case Right(_) => go()
          }
        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)
          go()
        case SResultNeedPackage(pkgId, context, _) =>
          throw Error.Internal(LookupError.MissingPackage.pretty(pkgId, context))
        case _: SResultScenarioGetParty =>
          throw Error.Internal("SResultScenarioGetParty in submission")
        case _: SResultScenarioPassTime =>
          throw Error.Internal("SResultScenarioPassTime in submission")
        case _: SResultScenarioSubmit =>
          throw Error.Internal("SResultScenarioSubmit in submission")
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
    def warningLog: WarningLog
  }

  final case class CurrentSubmission(
      commitLocation: Option[Location],
      ptx: IncompleteTransaction,
  )

  final case class ScenarioSuccess(
      ledger: ScenarioLedger,
      traceLog: TraceLog,
      warningLog: WarningLog,
      duration: Double,
      steps: Int,
      resultValue: SValue,
  ) extends ScenarioResult

  final case class ScenarioError(
      ledger: ScenarioLedger,
      traceLog: TraceLog,
      warningLog: WarningLog,
      currentSubmission: Option[CurrentSubmission],
      stackTrace: ImmArray[Location],
      error: Error,
  ) extends ScenarioResult
}
