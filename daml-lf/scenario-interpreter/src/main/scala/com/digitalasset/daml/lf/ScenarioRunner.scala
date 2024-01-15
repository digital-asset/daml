// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, Result, ResultDone, ResultError, ValueEnricher}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.language.{Ast, LanguageMajorVersion, LookupError}
import com.daml.lf.transaction.{GlobalKey, NodeId, SubmittedTransaction}
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.lf.speedy._
import com.daml.lf.speedy.SExpr.{SEApp, SEValue, SExpr}
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.IncompleteTransaction
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** Speedy scenario runner that uses the reference ledger.
  *
  * @constructor Creates a runner using an instance of [[Speedy.Machine]].
  */
final class ScenarioRunner private (
    machine: Speedy.ScenarioMachine,
    initialSeed: crypto.Hash,
    timeout: Duration,
) {
  import ScenarioRunner._

  private[this] val nextSeed: () => crypto.Hash = crypto.Hash.secureRandom(initialSeed)

  var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
  var currentSubmission: Option[CurrentSubmission] = None

  private def runUnsafe(implicit loggingContext: LoggingContext): ScenarioSuccess = {
    val isOverdue = TimeBomb(timeout).start()
    val startTime = System.nanoTime()
    var steps = 0

    @tailrec
    def innerLoop(
        result: SubmissionResult[ScenarioLedger.CommitResult]
    ): Either[SubmissionError, Commit[ScenarioLedger.CommitResult]] = {
      if (isOverdue()) throw scenario.Error.Timeout(timeout)
      result match {
        case commit @ Commit(_, _, _) => Right(commit)
        case err: SubmissionError => Left(err)
        case Interruption(continue) => innerLoop(continue())
      }
    }

    @tailrec
    def outerloop(): SValue = {
      if (isOverdue())
        throw scenario.Error.Timeout(timeout)

      steps += 1

      machine.run() match {

        case SResultQuestion(question) =>
          question match {

            case Question.Scenario.GetTime(callback) =>
              callback(ledger.currentTime)

            case Question.Scenario.PassTime(delta, callback) =>
              passTime(delta, callback)

            case Question.Scenario.GetParty(partyText, callback) =>
              getParty(partyText, callback)

            case Question.Scenario.Submit(committers, commands, location, mustFail, callback) =>
              val submitResult = innerLoop(
                submit(
                  compiledPackages = machine.compiledPackages,
                  ledger = ScenarioLedgerApi(ledger),
                  committers = committers,
                  readAs = Set.empty,
                  commands = SEValue(commands),
                  location = location,
                  seed = nextSeed(),
                  traceLog = machine.traceLog,
                  warningLog = machine.warningLog,
                )
              )

              if (mustFail) {
                submitResult match {
                  case Right(Commit(result, _, tx)) =>
                    currentSubmission = Some(CurrentSubmission(location, tx))
                    throw scenario.Error.MustFailSucceeded(result.richTransaction.transaction)
                  case Left(_) =>
                    ledger = ledger.insertAssertMustFail(committers, Set.empty, location)
                    callback(SValue.SUnit)
                }
              } else {
                submitResult match {
                  case Right(Commit(result, value, _)) =>
                    currentSubmission = None
                    ledger = result.newLedger
                    callback(value)
                  case Left(SubmissionError(err, tx)) =>
                    currentSubmission = Some(CurrentSubmission(location, tx))
                    throw err
                }
              }
          }
          outerloop()

        case SResultFinal(v) =>
          v

        case SResultInterruption =>
          outerloop()

        case SResultError(err) =>
          throw scenario.Error.RunnerException(err)

      }
    }

    val finalValue = outerloop()
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    ScenarioSuccess(
      ledger = ledger,
      traceLog = machine.traceLog,
      warningLog = machine.warningLog,
      profile = machine.profile,
      duration = diff,
      steps = steps,
      resultValue = finalValue,
    )
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
      machine: Speedy.ScenarioMachine,
      initialSeed: crypto.Hash,
      timeout: Duration,
  )(implicit loggingContext: LoggingContext): ScenarioResult = {
    val runner = new ScenarioRunner(machine, initialSeed, timeout)
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

  sealed abstract class SubmissionResult[+R] {
    @tailrec
    private[lf] final def resolve(): Either[SubmissionError, Commit[R]] = {
      this match {
        case commit: Commit[R] => Right(commit)
        case error: SubmissionError => Left(error)
        case Interruption(continue) => continue().resolve()
      }
    }
  }

  final case class Commit[+R](
      result: R,
      value: SValue,
      tx: IncompleteTransaction,
  ) extends SubmissionResult[R]

  final case class SubmissionError(error: Error, tx: IncompleteTransaction)
      extends SubmissionResult[Nothing]

  final case class Interruption[R](continue: () => SubmissionResult[R]) extends SubmissionResult[R]

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
        case ScenarioLedger.LookupOk(coinst) =>
          callback(coinst.toImplementation.toCreateNode.versionedCoinst)

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
            case ScenarioLedger.LookupOk(contract) =>
              if (!readers.intersect(contract.stakeholders).isEmpty)
                // Note that even with a successful global lookup
                // the callback can return false. This happens for a fetch-by-key
                // if the contract got archived in the meantime.
                // We discard the result here and rely on fetch-by-key
                // setting up the state such that continuing interpretation fails.
                discard(callback(Some(acoid)))
              else
                throw Error.ContractKeyNotVisible(acoid, gk, actAs, readAs, contract.stakeholders)
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
    val config = Engine.DevEngine(LanguageMajorVersion.V1).config
    val valueTranslator =
      new ValueTranslator(
        pkgInterface = compiledPackages.pkgInterface,
        requireV1ContractIdSuffix = config.requireSuffixedGlobalContractId,
      )
    def translateValue(typ: Ast.Type, value: Value): Result[SValue] =
      valueTranslator.strictTranslateValue(typ, value) match {
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
      packageResolution = Map.empty,
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
    )
    // TODO (drsk) validate and propagate errors back to submitter
    // https://github.com/digital-asset/daml/issues/14108
    val enricher = if (doEnrichment) new EnricherImpl(compiledPackages) else NoEnricher
    import enricher._

    def continue = () => go()

    @tailrec
    def go(): SubmissionResult[R] = {
      ledgerMachine.run() match {
        case SResultQuestion(question) =>
          question match {
            case _: Question.Update.NeedAuthority =>
              sys.error("choice authority not supported by scenarios")
            case Question.Update.NeedContract(coid, committers, callback) =>
              ledger.lookupContract(
                coid,
                committers,
                readAs,
                (vcoinst: VersionedContractInstance) => callback(vcoinst.unversioned),
              ) match {
                case Left(err) => SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                case Right(_) => go()
              }
            case Question.Update.NeedUpgradeVerification(_, _, _, _, callback) =>
              callback(None)
              go()
            case Question.Update.NeedKey(keyWithMaintainers, committers, callback) =>
              ledger.lookupKey(
                keyWithMaintainers.globalKey,
                committers,
                readAs,
                callback,
              ) match {
                case Left(err) => SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))
                case Right(_) => go()
              }
            case Question.Update.NeedTime(callback) =>
              callback(ledger.currentTime)
              go()
            case Question.Update.NeedPackageId(module @ _, pid0, callback) =>
              // TODO https://github.com/digital-asset/daml/issues/16154 (dynamic-exercise)
              // For now this just continues with the input package id
              callback(pid0)
              go()
            case res: Question.Update.NeedPackage =>
              throw Error.Internal(s"unexpected $res")
          }
        case SResultInterruption =>
          Interruption(continue)
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
      profile: Profile,
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
