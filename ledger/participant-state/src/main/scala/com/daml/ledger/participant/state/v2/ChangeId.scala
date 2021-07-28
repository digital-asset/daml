// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.data.Ref

/** Identifier for ledger changes used by command deduplication
  *
  * @see ReadService.stateUpdates for the command deduplication guarantee
  */
final case class ChangeId(
    private val applicationId: Ref.ApplicationId,
    private val commandId: Ref.CommandId,
    private val actAs: Set[Ref.Party],
)
