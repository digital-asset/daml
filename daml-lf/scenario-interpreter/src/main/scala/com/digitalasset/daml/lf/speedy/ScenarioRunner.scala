// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.types.Ledger._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Transaction._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey

//
// Speedy scenario runner that uses the reference ledger.
//

private case class SRunnerException(err: SError) extends RuntimeException(err.toString)

final case class ScenarioRunner(machine: Speedy.Machine) {
  var ledger: Ledger = Ledger.initialLedger(Time.Timestamp.Epoch)
  import scala.util.{Try, Success, Failure}

  def run(): Either[(SError, Ledger), (Double, Int, Ledger)] =
    Try(runUnsafe) match {
      case Failure(SRunnerException(err)) =>
        Left((err, ledger))
      case Failure(other) =>
        throw other
      case Success(res) =>
        Right(res)
    }

  private def runUnsafe(): (Double, Int, Ledger) = {
    // NOTE(JM): Written with an imperative loop and exceptions for speed
    // and so that we don't need to worry about stack usage.
    val startTime = System.nanoTime()
    var steps = 0
    while (!machine.isFinal) {
      //machine.print(steps)
      machine.step match {
        case SResultContinue =>
          steps += 1
        case SResultError(err) =>
          throw SRunnerException(err)

        case SResultMissingDefinition(ref, _) =>
          crash(s"definition $ref not found")

        case SResultNeedContract(coid, tid @ _, optCommitter, cbMissing, cbPresent) =>
          lookupContract(coid, optCommitter, cbMissing, cbPresent)

        case SResultNeedTime(callback) =>
          callback(ledger.currentTime)

        case SResultScenarioMustFail(tx, committer, callback) =>
          mustFail(tx, committer)
          callback(())

        case SResultScenarioCommit(value, tx, committer, callback) =>
          commit(value, tx, committer, callback)

        case SResultScenarioPassTime(delta, callback) =>
          passTime(delta, callback)

        case SResultScenarioInsertMustFail(committer, optLocation) =>
          ledger = ledger.insertAssertMustFail(committer, optLocation)

        case SResultScenarioGetParty(partyText, callback) =>
          getParty(partyText, callback)

        case SResultNeedKey(gk, cbMissing, cbPresent) =>
          lookupKey(gk, cbMissing, cbPresent)
      }
    }
    val endTime = System.nanoTime()
    val diff = (endTime - startTime) / 1000.0 / 1000.0
    (diff, steps, ledger)
  }

  private def crash(reason: String) =
    throw SRunnerException(SErrorCrash(reason))

  private def getParty(partyText: String, callback: Party => Unit) =
    SimpleString.fromString(partyText) match {
      case Right(s) => callback(s)
      case _ => throw SRunnerException(ScenarioErrorInvalidPartyName(partyText))
    }

  private def mustFail(tx: Transaction, committer: Party) = {
    // Update expression evaluated successfully,
    // however we might still have an authorization failure.
    if (Ledger
        .commitTransaction(
          committer = committer,
          effectiveAt = ledger.currentTime,
          optLocation = machine.commitLocation,
          tr = tx,
          l = ledger)
        .isRight) {
      throw SRunnerException(ScenarioErrorMustFailSucceeded(tx))
    }
    ledger = ledger.insertAssertMustFail(committer, machine.commitLocation)
  }

  private def commit(value: SValue, tx: Transaction, committer: Party, callback: SValue => Unit) = {
    Ledger.commitTransaction(
      committer = committer,
      effectiveAt = ledger.currentTime,
      optLocation = machine.commitLocation,
      tr = tx,
      l = ledger
    ) match {
      case Left(fas) =>
        throw SRunnerException(ScenarioErrorCommitError(fas))
      case Right(result) =>
        ledger = result.newLedger
        callback(
          value
            .mapContractId(
              coid =>
                Ledger.contractIdToAbsoluteContractId(
                  Ledger.makeCommitPrefix(result.transactionId),
                  coid)))
    }
  }

  private def passTime(delta: Long, callback: Time.Timestamp => Unit) = {
    ledger = ledger.passTime(delta)
    callback(ledger.currentTime)
  }

  private def lookupContract(
      acoid: AbsoluteContractId,
      optCommitter: Option[Party],
      cbMissing: Unit => Boolean,
      cbPresent: ContractInst[Value[AbsoluteContractId]] => Unit) = {

    val committer = optCommitter.getOrElse(crash("committer missing"))
    val effectiveAt = ledger.currentTime

    def missingWith(err: SError) =
      if (!cbMissing(()))
        throw SRunnerException(err)

    ledger.lookupGlobalContract(view = ParticipantView(committer), effectiveAt = effectiveAt, acoid) match {
      case LookupOk(_, coinst) =>
        cbPresent(coinst)

      case LookupContractNotFound(coid) =>
        // This should never happen, hence we don't have a specific
        // error for this.
        missingWith(SErrorCrash(s"contract $coid not found"))

      case LookupContractNotEffective(coid, tid, effectiveAt) =>
        missingWith(ScenarioErrorContractNotEffective(coid, tid, effectiveAt))

      case LookupContractNotActive(coid, tid, consumedBy) =>
        missingWith(ScenarioErrorContractNotActive(coid, tid, consumedBy))

      case LookupContractNotVisible(coid, tid, observers) =>
        missingWith(ScenarioErrorContractNotVisible(coid, tid, committer, observers))
    }
  }

  private def lookupKey(
      gk: GlobalKey,
      cbMissing: Unit => Boolean,
      cbPresent: AbsoluteContractId => Unit) = {
    def missingWith(err: SError) =
      if (!cbMissing(())) {
        throw SRunnerException(err)
      }
    ledger.ledgerData.activeKeys.get(gk) match {
      case None => missingWith(SErrorCrash(s"Key $gk not found"))
      case Some(acoid) => cbPresent(acoid)
    }
  }
}
