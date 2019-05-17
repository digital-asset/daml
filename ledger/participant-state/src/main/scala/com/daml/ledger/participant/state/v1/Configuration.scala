// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  */
final case class Configuration(
    /** The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands. */
    timeModel: TimeModel
)
