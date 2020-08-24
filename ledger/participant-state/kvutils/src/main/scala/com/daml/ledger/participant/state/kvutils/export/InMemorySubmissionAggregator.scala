// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator.{Data, WriteSet}

import scala.collection.mutable

final class InMemorySubmissionAggregator extends SubmissionAggregator {

  import InMemorySubmissionAggregator._

  private val buffer = mutable.ListBuffer.empty[Data]

  override def addChild(): WriteSet = new InMemoryWriteSet(buffer)

  override def finish(): Seq[Data] = buffer
}

object InMemorySubmissionAggregator {

  final class InMemoryWriteSet(buffer: mutable.Buffer[Data]) extends WriteSet {
    override def +=(data: Data): Unit = buffer.synchronized {
      buffer += data
      ()
    }

    override def ++=(data: Iterable[Data]): Unit = buffer.synchronized {
      buffer ++= data
      ()
    }
  }

}
