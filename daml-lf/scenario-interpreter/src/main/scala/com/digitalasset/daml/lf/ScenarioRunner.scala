// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.speedy.SExpr.{SExpr, SEValue, SEApp}
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.IncompleteTransaction
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** Speedy scenario runner that uses the reference ledger.
  *
  * @constructor Creates a runner using an instance of [[Speedy.Machine]].
  */
final class ScenarioRunner private (
    machine: Speedy.ScenarioMachine,
    initialSeed: crypto.Hash,
) {
  import ScenarioRunner._

  private[this] val nextSeed: () => crypto.Hash = crypto.Hash.secureRandom(initialSeed)

  var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  var currentSubmission: Option[CurrentSubmission] = None

  private def runUnsafe(implicit loggingContext: LoggingContext): ScenarioSuccess = {
    // NOTE(JM): Written with an imperative loop and exceptions for speed
    // and so that we don't need to worry about stack usage.
    val startTime = System.nanoTime()
    var steps = 0
    var finalValue: SValue = null
    while (finalValue == null) {
      // machine.print(steps)
      steps += 1 // this counts the number of external `Need` interactions
      val res: SResult = machine.run()
      res match {
        case SResultFinal(v) =>
          finalValue = v

        case SResultError(err) =>
          throw scenario.Error.RunnerException(err)

        case SResultScenarioGetTime(callback) =>
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
            SEValue(commands),
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

        case _: SResultNeedPackage | _: SResultNeedContract | _: SResultNeedKey |
            _: SResultNeedTime =>
          crash(s"unexpected $res")
      }
    }
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    ScenarioSuccess(ledger, machine.traceLog, machine.warningLog, diff, steps, finalValue)
  }

  private def getParty(partyText: String, callback: Party => Unit) =
    Party.fromString(partyText) match {
      case Right(s) => callback(s)
      case Left(msg) => throw Error.InvalidPartyName(partyText, msg)
    }

  private def passTime(delta: Long, callback: Time.Timestamp => Unit) = {
    ledger = ledger.passTime(delta)
    callback(ledger.currentTime)
  }
}

private[lf] object ScenarioRunner {

  def run(
      buildMachine: () => Speedy.ScenarioMachine,
      initialSeed: crypto.Hash,
  )(implicit loggingContext: LoggingContext): ScenarioResult = {
    val machine = buildMachine()
    val runner = new ScenarioRunner(machine, initialSeed)
    handleUnsafe(runner.runUnsafe) match {
      case Left(err) =>
        val stackTrace =
          machine.getLastLocation match {
            case None => ImmArray()
            case Some(location) => ImmArray(location)
          }
        ScenarioError(
          runner.ledger,
          machine.traceLog,
          machine.warningLog,
          runner.currentSubmission,
          stackTrace,
          err,
        )
      case Right(t) => t
    }
  }

  private def crash(reason: String) =
    throw Error.Internal(reason)

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
  private[lf] abstract class LedgerApi[R] {
    def lookupContract(
        coid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        cbPresent: VersionedContractInstance => Unit,
    ): Either[Error, Unit]
    def lookupKey(
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

  private[lf] case class ScenarioLedgerApi(ledger: ScenarioLedger)
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
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Either[Error, Unit] =
      handleUnsafe(lookupKeyUnsafe(gk, actAs, readAs, callback))

    private def lookupKeyUnsafe(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: Option[ContractId] => Boolean,
    ): Unit = {

      val effectiveAt = ledger.currentTime
      val readers = actAs union readAs

      def missingWith(err: Error) =
        if (!callback(None)) {
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

  private[this] abstract class Enricher {
    def enrich(tx: SubmittedTransaction): SubmittedTransaction
    def enrich(tx: IncompleteTransaction): IncompleteTransaction
  }

  private[this] object NoEnricher extends Enricher {
    override def enrich(tx: SubmittedTransaction): SubmittedTransaction = tx
    override def enrich(tx: IncompleteTransaction): IncompleteTransaction = tx
  }

  private[this] class EnricherImpl(compiledPackages: CompiledPackages) extends Enricher {
    val config = Engine.DevEngine().config
    val valueTranslator =
      new ValueTranslator(
        pkgInterface = compiledPackages.pkgInterface,
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
    override def enrich(tx: SubmittedTransaction): SubmittedTransaction =
      SubmittedTransaction(consume(enricher.enrichVersionedTransaction(tx)))
    override def enrich(tx: IncompleteTransaction): IncompleteTransaction =
      consume(enricher.enrichIncompleteTransaction(tx))
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
  )(implicit loggingContext: LoggingContext): SubmissionResult[R] = {
    val ledgerMachine = Speedy.UpdateMachine(
      compiledPackages = compiledPackages,
      submissionTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SEApp(commands, Array(SValue.SToken)),
      committers = committers,
      readAs = readAs,
      traceLog = traceLog,
      warningLog = warningLog,
      commitLocation = location,
      limits = interpretation.Limits.Lenient,
      disclosedContracts = ImmArray.Empty,
    )
    // TODO (drsk) validate and propagate errors back to submitter
    // https://github.com/digital-asset/daml/issues/14108
    val enricher = if (doEnrichment) new EnricherImpl(compiledPackages) else NoEnricher
    import enricher._

    @tailrec
    def go(): SubmissionResult[R] = {
      ledgerMachine.run() match {
        case SResult.SResultFinal(resultValue) =>
          ledgerMachine.finish match {
            case Right(Speedy.UpdateMachine.Result(tx, locationInfo, _, _, _)) =>
              ledger.commit(committers, readAs, location, enrich(tx), locationInfo) match {
                case Left(err) =>
                  SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                case Right(r) =>
                  Commit(r, resultValue, enrich(ledgerMachine.incompleteTransaction))
              }
            case Left(err) =>
              throw err
          }
        case SResultError(err) =>
          SubmissionError(Error.RunnerException(err), enrich(ledgerMachine.incompleteTransaction))
        case SResultNeedContract(coid, committers, callback) =>
          ledger.lookupContract(
            coid,
            committers,
            readAs,
            (vcoinst: VersionedContractInstance) => callback(vcoinst.unversioned),
          ) match {
            case Left(err) => SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
            case Right(_) => go()
          }
        case SResultNeedKey(keyWithMaintainers, committers, callback) =>
          ledger.lookupKey(
            keyWithMaintainers.globalKey,
            committers,
            readAs,
            callback,
          ) match {
            case Left(err) => SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
            case Right(_) => go()
          }
        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)
          go()
        case res @ (_: SResultNeedPackage | _: SResultScenarioGetParty |
            _: SResultScenarioPassTime | _: SResultScenarioSubmit | _: SResultScenarioGetTime) =>
          throw Error.Internal(s"unexpected $res")
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
