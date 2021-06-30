// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.speedy.PartialTransaction

import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc._

private[lf] object Pretty {

  import speedy.Pretty._

  def prettyError(err: Error, optPtx: Option[PartialTransaction] = None): Doc =
    err match {
      case Error.RunnerException(serror) =>
        speedy.Pretty.prettyError(serror, optPtx)
      case Error.Internal(msg) =>
        text(s"CRASH: $msg ")
      case Error.ContractNotEffective(coid, tid, effectiveAt) =>
        text(s"Scenario failed due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") &
          text(s"that becomes effective at $effectiveAt")
      case Error.ContractNotActive(coid, tid, consumedBy) =>
        text("Scenario failed due to a fetch of a consumed contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The contract had been consumed in transaction") & prettyEventId(consumedBy)
      case Error.ContractNotVisible(coid, tid, actAs, readAs, observers) =>
        text("Scenario failed due to the failure to fetch the contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The contract had not been disclosed to the reading parties:") &
          text("actAs:") & intercalate(comma + space, actAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          text("readAs:") & intercalate(comma + space, readAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) +
          char('.') / text("The contract had been disclosed to:") & intercalate(
            comma + space,
            observers.map(prettyParty),
          ) + char('.')
      case Error.ContractKeyNotVisible(coid, tid, key, actAs, readAs, stakeholders) =>
        text("Scenario failed due to the failure to fetch the contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(") associated with key ") +
          prettyValue(false)(key) &
          text("The contract had not been disclosed to the reading parties:") &
          text("actAs:") & intercalate(comma + space, actAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          text("readAs:") & intercalate(comma + space, readAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) +
          char('.') / text("Stakeholders:") & intercalate(
            comma + space,
            stakeholders.map(prettyParty),
          ) + char('.')

      case Error.CommitError(ScenarioLedger.CommitError.UniqueKeyViolation(err)) =>
        (text("Scenario failed due to unique key violation for key:") &
          prettyValue(false)(err.key) & text("for template") & prettyIdentifier(err.templateId))

      case Error.MustFailSucceeded(tx @ _) =>
        // TODO(JM): Further info needed. Location annotations?
        text("Scenario failed due to a mustfailAt that succeeded.")

      case Error.InvalidPartyName(_, msg) => text(s"Error: Invalid party: $msg")

      case Error.PartyAlreadyExists(party) =>
        text(s"Error: Tried to allocate a party that already exists: $party")
    }

}
