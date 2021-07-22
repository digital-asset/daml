// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Instant

import com.daml.lf.data.Ref

/** A temporary class, designed to aid in modifying the participant integration API code to use v2.
  *
  * This file should not last long.
  */
final case class CompletionInfo(
    actAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    deduplicateUntil: Instant,
) {
  def toSubmitterInfo: SubmitterInfo =
    SubmitterInfo(actAs, applicationId, commandId, deduplicateUntil)
}
