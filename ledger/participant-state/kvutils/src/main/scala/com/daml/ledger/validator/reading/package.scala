// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}

package object reading {

  type LedgerStateReader = StateReader[Raw.Key, Option[Raw.Value]]

  type DamlLedgerStateReader = StateReader[DamlStateKey, Option[DamlStateValue]]

}
