// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator.WriteSetBuilder

import scala.collection.mutable

final class InMemorySubmissionAggregator(submissionInfo: SubmissionInfo, writer: LedgerDataWriter)
    extends SubmissionAggregator {

  import InMemorySubmissionAggregator._

  private val buffer = mutable.ListBuffer.empty[WriteItem]

  override def addChild(): WriteSetBuilder = new InMemoryWriteSetBuilder(buffer)

  override def finish(): Unit = writer.write(submissionInfo, buffer)
}

object InMemorySubmissionAggregator {

  final class InMemoryWriteSetBuilder(buffer: mutable.Buffer[WriteItem]) extends WriteSetBuilder {
    override def +=(data: WriteItem): Unit = buffer.synchronized {
      buffer += data
      ()
    }

    override def ++=(data: Iterable[WriteItem]): Unit = buffer.synchronized {
      buffer ++= data
      ()
    }
  }

}
