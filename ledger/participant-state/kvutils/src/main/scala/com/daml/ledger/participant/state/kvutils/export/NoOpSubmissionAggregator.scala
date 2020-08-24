// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator.WriteSetBuilder

object NoOpSubmissionAggregator extends SubmissionAggregator {
  override def addChild(): WriteSetBuilder = NoOpWriteSetBuilder

  override def finish(): Unit = ()

  object NoOpWriteSetBuilder extends WriteSetBuilder {
    override def +=(data: WriteItem): Unit = ()

    override def ++=(data: Iterable[WriteItem]): Unit = ()
  }

}
