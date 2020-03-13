// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

/** The participant-state key-value utilities provide methods to succintly implement
  * [[com.daml.ledger.participant.state.v1.ReadService]] and
  * [[com.daml.ledger.participant.state.v1.WriteService]] on top of ledger's that provide a key-value state storage.
  *
  * The key-value utilities are based around the concept of modelling the ledger around
  * an abstract state that can be described as the tuple `(logEntryIds, logEntryMap, kvState)`,
  * of type `(List[DamlLogEntryId], Map[DamlLogEntryId, DamlLogEntry], Map[DamlStateKey, DamlStateValue])`.
  *
  * `logEntryIds` describes the ordering of log entries. The `logEntryMap` contains the data for the log entries.
  * This map is expected to be append-only and existing entries are never modified or removed.
  * `kvState` describes auxilliary mutable state which may be created as part of one log entry and mutated by a later one.
  * (e.g. a log entry might describe a DAML transaction containing contracts and the auxilliary mutable data may
  * describe their activeness).
  *
  * While these can be represented in a key-value store directly, some implementations may
  * provide the ordering of log entries from outside the state (e.g. via a transaction chain).
  * The distinction between DAML log entries and DAML state values is that log entries are immutable,
  * and that their keys are not necessarily known beforehand, which is why the implementation deals
  * with them separately, even though both log entries and DAML state values may live in the same storage.
  */
package object kvutils {
  import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}

  type DamlStateMap = Map[DamlStateKey, Option[DamlStateValue]]

}
