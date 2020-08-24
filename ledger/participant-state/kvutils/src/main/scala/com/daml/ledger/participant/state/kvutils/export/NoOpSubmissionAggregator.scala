// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator.{Data, WriteSet}

object NoOpSubmissionAggregator extends SubmissionAggregator {
  override def addChild(): WriteSet = NoOpWriteSet

  override def finish(): Seq[Data] = Seq.empty

  object NoOpWriteSet extends WriteSet {
    override def +=(data: Data): Unit = ()

    override def ++=(data: Iterable[Data]): Unit = ()
  }

}
