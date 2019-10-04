package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}

package object committer {

  type DamlStateMap = Map[DamlStateKey, DamlStateValue]

}
