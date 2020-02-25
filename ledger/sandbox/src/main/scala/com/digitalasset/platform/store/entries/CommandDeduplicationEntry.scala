// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.entries

import java.time.Instant

import com.daml.ledger.participant.state.index.v2.CommandSubmissionResult

final case class CommandDeduplicationEntry(
    deduplicationKey: String,
    submittedAt: Instant,
    ttl: Instant,
    result: Option[CommandSubmissionResult],
)
