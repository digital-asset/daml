// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator._

trait SubmissionAggregator {
  def addChild(): WriteSetBuilder

  def finish(): Unit
}

object SubmissionAggregator {

  trait WriteSetBuilder {
    def +=(data: WriteItem): Unit

    def ++=(data: Iterable[WriteItem]): Unit
  }

}
