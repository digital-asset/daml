// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.Node.GlobalKey

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
    partyNameMangler: (String => String) = identity) {
  var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)

  import scala.util.{Try, Success, Failure}

  def run(): Either[(SError, ScenarioLedger), (Double, Int, ScenarioLedger, SValue)] =
    Try(runUnsafe) match {
      case Failure(SRunnerException(err)) =>
        Left((err, ledger))
      case Failure(other) =>
        throw other
      case Success(res) =>
        Right(res)
    }

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

        case SResultNeedPackage(pkgId, _) =>
          crash(s"package $pkgId not found")

        case SResultNeedContract(coid, tid @ _, committers, cbMissing, cbPresent) =>
          lookupContract(coid, committers, cbMissing, cbPresent)

        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)

        case SResultScenarioMustFail(tx, committers, callback) =>
          mustFail(tx, committers)
          callback(())

        case SResultScenarioCommit(value, tx, committers, callback) =>
          commit(value, tx, committers, callback)

        case SResultScenarioPassTime(delta, callback) =>
          passTime(delta, callback)

        case SResultScenarioInsertMustFail(committers, optLocation) => {
          val committer =
            if (committers.size == 1) committers.head else crashTooManyCommitters(committers)

          ledger = ledger.insertAssertMustFail(committer, optLocation)
        }

        case SResultScenarioGetParty(partyText, callback) =>
          getParty(partyText, callback)

        case SResultNeedKey(gk, committers, cb) =>
          lookupKey(gk, committers, cb)
      }
    }
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    (diff, steps, ledger, finalValue)
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

  private def mustFail(tx: Tx.SubmittedTransaction, committers: Set[Party]) = {
    // Update expression evaluated successfully,
    // however we might still have an authorization failure.
    val committer =
      if (committers.size == 1) committers.head else crashTooManyCommitters(committers)

    if (ScenarioLedger
        .commitTransaction(
          committer = committer,
          effectiveAt = ledger.currentTime,
          optLocation = machine.commitLocation,
          tx = tx,
          l = ledger)
        .isRight) {
      throw SRunnerException(ScenarioErrorMustFailSucceeded(tx))
    }
    ledger = ledger.insertAssertMustFail(committer, machine.commitLocation)
  }

  private def commit(
      value: SValue,
      tx: Tx.SubmittedTransaction,
      committers: Set[Party],
      callback: SValue => Unit) = {
    val committer =
      if (committers.size == 1) committers.head else crashTooManyCommitters(committers)

    ScenarioLedger.commitTransaction(
      committer = committer,
      effectiveAt = ledger.currentTime,
      optLocation = machine.commitLocation,
      tx = tx,
      l = ledger
    ) match {
      case Left(fas) =>
        throw SRunnerException(ScenarioErrorCommitError(fas))
      case Right(result) =>
        ledger = result.newLedger
        callback(value)
    }
  }

  private def passTime(delta: Long, callback: Time.Timestamp => Unit) = {
    ledger = ledger.passTime(delta)
    callback(ledger.currentTime)
  }

  private def lookupContract(
      acoid: ContractId,
      committers: Set[Party],
      cbMissing: Unit => Boolean,
      cbPresent: ContractInst[Tx.Value[ContractId]] => Unit) = {

    val committer =
      if (committers.size == 1) committers.head else crashTooManyCommitters(committers)
    val effectiveAt = ledger.currentTime

    def missingWith(err: SError) =
      if (!cbMissing(()))
        throw SRunnerException(err)

    ledger.lookupGlobalContract(
      view = ScenarioLedger.ParticipantView(committer),
      effectiveAt = effectiveAt,
      acoid) match {
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

      case ScenarioLedger.LookupContractNotVisible(coid, tid, observers) =>
        missingWith(ScenarioErrorContractNotVisible(coid, tid, committer, observers))
    }
  }

  private def lookupKey(
      gk: GlobalKey,
      committers: Set[Party],
      cb: SKeyLookupResult => Boolean,
  ): Unit = {
    val committer =
      if (committers.size == 1) committers.head else crashTooManyCommitters(committers)
    val effectiveAt = ledger.currentTime

    def missingWith(err: SError) =
      if (!cb(SKeyLookupResult.NotFound))
        throw SRunnerException(err)

    ledger.ledgerData.activeKeys.get(gk) match {
      case None =>
        missingWith(SErrorCrash(s"Key $gk not found"))
      case Some(acoid) =>
        ledger.lookupGlobalContract(
          view = ScenarioLedger.ParticipantView(committer),
          effectiveAt = effectiveAt,
          acoid) match {
          case ScenarioLedger.LookupOk(_, _, stakeholders) =>
            if (stakeholders.contains(committer))
              cb(SKeyLookupResult.Found(acoid))
            else if (!cb(SKeyLookupResult.NotVisible))
              missingWith(ScenarioErrorContractKeyNotVisible(acoid, gk, committer, stakeholders))
            ()
          case ScenarioLedger.LookupContractNotFound(coid) =>
            missingWith(SErrorCrash(s"contract $coid not found, but we found its key!"))
          case ScenarioLedger.LookupContractNotEffective(_, _, _) =>
            missingWith(SErrorCrash(s"contract $acoid not effective, but we found its key!"))
          case ScenarioLedger.LookupContractNotActive(_, _, _) =>
            missingWith(SErrorCrash(s"contract $acoid not active, but we found its key!"))
          case ScenarioLedger.LookupContractNotVisible(coid, tid, observers) =>
            if (!cb(SKeyLookupResult.NotVisible))
              missingWith(ScenarioErrorContractKeyNotVisible(coid, gk, committer, observers))
        }
    }
  }

  private def crashTooManyCommitters(committers: Set[Party]) =
    crash(s"Expecting one committer for scenario action, but got $committers")

}
