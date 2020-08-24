// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

trait SubmissionAggregator {
  def addChild(): WriteSet

  def finish(): Unit
}

object SubmissionAggregator {
  type Data = (Key, Value)

  trait WriteSet {
    def +=(data: Data): Unit

    def ++=(data: Iterable[Data]): Unit
  }

}
