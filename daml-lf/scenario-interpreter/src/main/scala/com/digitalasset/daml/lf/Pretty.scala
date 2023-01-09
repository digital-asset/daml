// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc._

private[lf] object Pretty {

  import speedy.Pretty._

  def prettyError(err: Error): Doc =
    err match {
      case Error.RunnerException(serror) =>
        speedy.Pretty.prettyError(serror)
      case Error.Internal(msg) =>
        text(s"CRASH: $msg ")
      case Error.ContractNotEffective(coid, tid, effectiveAt) =>
        text(s"Scenario failed due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") &
          text(s"that becomes effective at $effectiveAt")
      case Error.ContractNotActive(coid, tid, Some(consumedBy)) =>
        text("Scenario failed due to a fetch of a consumed contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The contract had been consumed in transaction") & prettyEventId(consumedBy)
      case Error.ContractNotActive(coid, tid, None) =>
        text("Scenario failed due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The create of this contract has been rolled back")
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
      case Error.ContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders) =>
        text("Scenario failed due to the failure to fetch the contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(gk.templateId)) + text(") associated with key ") +
          prettyValue(false)(gk.key) &
          text("The contract had not been disclosed to the reading parties:") &
          text("actAs:") & intercalate(comma + space, actAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          text("readAs:") & intercalate(comma + space, readAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) +
          char('.') / text("Stakeholders:") & intercalate(
            comma + space,
            stakeholders.map(prettyParty),
          ) + char('.')

      case Error.CommitError(ScenarioLedger.CommitError.UniqueKeyViolation(gk)) =>
        (text("Scenario failed due to unique key violation for key:") & prettyValue(false)(
          gk.gk.key
        ) & text(
          "for template"
        ) & prettyIdentifier(gk.gk.templateId))

      case Error.MustFailSucceeded(tx @ _) =>
        // TODO(JM): Further info needed. Location annotations?
        text("Scenario failed due to a mustfailAt that succeeded.")

      case Error.InvalidPartyName(_, msg) => text(s"Error: Invalid party: $msg")

      case Error.PartyAlreadyExists(party) =>
        text(s"Error: Tried to allocate a party that already exists: $party")

      case Error.PartiesNotAllocated(parties) =>
        text(s"Error: Tried to submit a command for parties that have not been allocated:") &
          intercalate(comma + space, parties.map(prettyParty))
    }

}
