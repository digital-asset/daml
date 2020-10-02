// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.daml.lf.data.Time.Timestamp

/** The initial conditions of the ledger before anything has been committed.
  *
  * @param ledgerId: The static ledger identifier.
  * @param config: The initial ledger configuration
  * @param initialRecordTime: The initial record time prior to any [[Update]] event.
  */
final case class LedgerInitialConditions(
    ledgerId: LedgerId,
    config: Configuration,
    initialRecordTime: Timestamp
)
