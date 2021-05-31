// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.logging.LoggingContext

private[committer] trait CommitStep[PartialResult] {
  def apply(
      context: CommitContext,
      input: PartialResult,
  )(implicit loggingContext: LoggingContext): StepResult[PartialResult]
}
