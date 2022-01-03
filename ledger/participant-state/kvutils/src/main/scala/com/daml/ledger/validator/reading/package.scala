// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}

package object reading {

  type LedgerStateReader = StateReader[Raw.StateKey, Option[Raw.Envelope]]

  type DamlLedgerStateReader = StateReader[DamlStateKey, Option[DamlStateValue]]

}
