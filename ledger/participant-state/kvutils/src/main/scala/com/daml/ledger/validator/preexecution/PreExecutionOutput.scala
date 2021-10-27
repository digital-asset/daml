// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

sealed case class PreExecutionOutput[+ReadSet, +WriteSet](
    minRecordTime: Option[Timestamp],
    maxRecordTime: Option[Timestamp],
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    readSet: ReadSet,
    involvedParticipants: Set[Ref.ParticipantId],
)
