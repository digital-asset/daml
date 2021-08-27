// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.codahale.metrics.MetricRegistry
import com.daml.concurrent.{Future => DamlFuture}
import com.daml.ledger.on.sql.Database
import com.daml.ledger.on.sql.Database.RDBMS
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.resources.ResourceContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers._

trait QueryBehaviors { this: AsyncFlatSpec =>
  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

  def queriesOnInsertion(rdbms: RDBMS, jdbcUrl: => String): Unit = {
    it should "insert the first log entry with an ID of 1" in {
      val metrics = new Metrics(new MetricRegistry)
      Database.SingleConnectionDatabase
        .owner(rdbms, jdbcUrl, metrics)
        .map(_.migrate())
        .use { database =>
          database
            .inWriteTransaction("insert_log_entry") { queries =>
              DamlFuture.fromTry(for {
                indexBeforeInserting <- queries.selectLatestLogEntryId()
                insertedIndex <- queries.insertRecordIntoLog(
                  key = Raw.LogEntryId(ByteString.copyFromUtf8("log entry ID")),
                  value = Raw.Envelope(ByteString.copyFromUtf8("log entry value")),
                )
                indexAfterInserting <- queries.selectLatestLogEntryId()
              } yield {
                indexBeforeInserting should be(None)
                insertedIndex should be(1)
                indexAfterInserting should be(Some(1))
              })
            }
            .removeExecutionContext
        }
    }
  }
}
