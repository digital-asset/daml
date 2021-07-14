// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import org.openjdk.jmh.annotations._
import scalaz.std.list._
import spray.json._
import spray.json.DefaultJsonProtocol._

class InsertBenchmark extends ContractDaoBenchmark {
  @Param(Array("1", "3", "5", "7", "9"))
  var batches: Int = _

  @Param(Array("1000"))
  var numContracts: Int = _

  private var contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] = _

  private var contractCids: List[String] = _

  @Setup(Level.Trial)
  override def setup(): Unit = {
    super.setup()
    contracts = (1 until numContracts + 1).map { i =>
      // Use negative cids to avoid collisions with other contracts
      contract(-i, "Alice")
    }.toList

    contractCids = contracts.map(_.contractId)

    (0 until batches).foreach { batch =>
      insertBatch("Alice", batch * batchSize)
    }
    ()
  }

  @TearDown(Level.Invocation)
  def dropContracts: Unit = {
    val deleted = dao.transact(dao.jdbcDriver.queries.deleteContracts(contractCids)).unsafeRunSync()
    assert(deleted == numContracts)
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime))
  def run(): Unit = {
    val driver: SupportedJdbcDriver = dao.jdbcDriver
    import driver._
    val inserted = dao.transact(driver.queries.insertContracts(contracts)).unsafeRunSync()
    assert(inserted == numContracts)
  }
}
