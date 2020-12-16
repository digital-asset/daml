// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

package object reading {

  type LedgerStateReader = StateReader[Key, Option[Value]]

  type DamlLedgerStateReader = StateReader[DamlStateKey, Option[DamlStateValue]]

}
