// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.`Bytes Ordering`
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator.WriteSetBuilder
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.mutable

final class InMemorySubmissionAggregator(submissionInfo: SubmissionInfo, writer: LedgerDataWriter)
    extends SubmissionAggregator {

  import InMemorySubmissionAggregator._

  private val aggregate = mutable.SortedMap.empty[Key, Value]

  override def addChild(): WriteSetBuilder = new InMemoryWriteSetBuilder(aggregate)

  override def finish(): Unit =
    writer.write(submissionInfo, aggregate)
}

object InMemorySubmissionAggregator {

  final class InMemoryWriteSetBuilder private[InMemorySubmissionAggregator] (
      aggregate: mutable.SortedMap[Key, Value],
  ) extends WriteSetBuilder {
    override def +=(data: WriteItem): Unit = aggregate.synchronized {
      aggregate += data
      ()
    }

    override def ++=(data: Iterable[WriteItem]): Unit = aggregate.synchronized {
      aggregate ++= data
      ()
    }
  }

}
