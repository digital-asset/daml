package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}

object Common {
  type DamlStateMap = Map[DamlStateKey, DamlStateValue]

  // A check result, which is either a rejection or passing check with associated new state.
  type CheckResult = Either[DamlLogEntry, DamlStateMap]
  def pass(state: (DamlStateKey, DamlStateValue)*): CheckResult = Right(state.toMap)

}
