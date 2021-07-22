// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.lf.data.Ref

sealed case class PreExecutionOutput[+ReadSet, +WriteSet](
    minRecordTime: Option[Instant],
    maxRecordTime: Option[Instant],
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    readSet: ReadSet,
    involvedParticipants: Set[Ref.ParticipantId],
)
