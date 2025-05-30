// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.daml.lf.language.Ast.PackageMetadata
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
        text(s"Script failed due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") &
          text(s"that becomes effective at $effectiveAt")
      case Error.ContractNotActive(coid, tid, Some(consumedBy)) =>
        text("Script failed due to a fetch of a consumed contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The contract had been consumed in transaction") & prettyEventId(consumedBy)
      case Error.ContractNotActive(coid, tid, None) =>
        text("Script failed due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
          text("The create of this contract has been rolled back")
      case Error.ContractNotVisible(coid, tid, actAs, readAs, observers) =>
        text("Script failed due to the failure to fetch the contract") & prettyContractId(coid) &
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
        text("Script failed due to the failure to fetch the contract") & prettyContractId(coid) &
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

      case Error.CommitError(IdeLedger.CommitError.UniqueKeyViolation(gk)) =>
        (text("Script failed due to unique key violation for key:") & prettyValue(false)(
          gk.gk.key
        ) & text(
          "for template"
        ) & prettyIdentifier(gk.gk.templateId))

      case Error.MustFailSucceeded(tx @ _) =>
        // TODO(JM): Further info needed. Location annotations?
        text("Script failed due to a mustfailAt that succeeded.")

      case Error.InvalidPartyName(_, msg) => text(s"Error: Invalid party: $msg")

      case Error.PartyAlreadyExists(party) =>
        text(s"Error: Tried to allocate a party that already exists: $party")

      case Error.PartiesNotAllocated(parties) =>
        text(s"Error: Tried to submit a command for parties that have not been allocated:") &
          intercalate(comma + space, parties.map(prettyParty))

      case Error.Timeout(timeout) =>
        text(s"Timeout: evaluation needed more that ${timeout.toSeconds}s to complete")

      case Error.CanceledByRequest() =>
        text("Evaluation was cancelled because the test was changed and rerun in a new thread.")

      case Error.LookupError(err, packageMetadata, packageId) =>
        val packageName = packageMetadata.fold(packageId.toString)({
          case PackageMetadata(name, version, _) => s"$name-$version"
        })
        text(
          s"Error: ${err.pretty}\nin package ${packageName}"
        )
      case Error.DisclosureDecoding(message) =>
        // TODO https://github.com/digital-asset/daml/issues/17647
        // improve error report
        text(s"decoding of disclosure fails: ${message}")
    }

}
