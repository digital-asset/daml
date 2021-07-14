// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import scalaz.OneAnd
import doobie.implicits._
import com.daml.http.domain.{Party}
import org.openjdk.jmh.annotations._

class QueryBenchmark extends ContractDaoBenchmark {
  @Param(Array("10"))
  var batches: Int = _

  @Setup(Level.Trial)
  override def setup(): Unit = {
    super.setup()
    (0 until batches - 1).foreach { batch =>
      insertBatch("Alice", batch * batchSize)
    }
    insertBatch("Bob", (batches - 1) * batchSize)
    ()
  }

  @Benchmark
  def run(): Unit = {
    implicit val driver: SupportedJdbcDriver = dao.jdbcDriver
    val result = dao
      .transact(ContractDao.selectContracts(OneAnd(Party("Bob"), Set.empty), tpid, fr"1 = 1"))
      .unsafeRunSync()
    assert(result.size == batchSize)
  }
}
