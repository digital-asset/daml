// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

sealed trait DeduplicationConversionFailure

object DeduplicationConversionFailure {
  case object CompletionAtOffsetNotFound extends DeduplicationConversionFailure
  case object CompletionOffsetNotMatching extends DeduplicationConversionFailure
  case object CompletionRecordTimeNotAvailable extends DeduplicationConversionFailure
  case object CompletionCheckpointNotAvailable extends DeduplicationConversionFailure
}
