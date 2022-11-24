// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.ContractTypeId
import org.openjdk.jmh.annotations._
import scalaz.std.list._
import spray.json._
import spray.json.DefaultJsonProtocol._

trait InsertBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("1", "3", "5", "7", "9"))
  var batches: Int = _

  @Param(Array("1000"))
  var numContracts: Int = _

  private var contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] = _

  private var contractCids: Set[String] = _

  private var tpid: SurrogateTpId = _

  @Setup(Level.Trial)
  override def setup(): Unit = {
    super.setup()
    tpid = insertTemplate(ContractTypeId.Template("-pkg-", "M", "T"))
    contracts = (1 until numContracts + 1).map { i =>
      // Use negative cids to avoid collisions with other contracts
      contract(-i, "Alice", tpid)
    }.toList

    contractCids = contracts.view.map(_.contractId).toSet

    (0 until batches).foreach { batch =>
      insertBatch("Alice", tpid, batch * batchSize)
    }
    ()
  }

  @TearDown(Level.Invocation)
  def dropContracts(): Unit = {
    val deleted = dao.transact(queries.deleteContracts(Map(tpid -> contractCids))).unsafeRunSync()
    assert(deleted == numContracts)
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime))
  def run(): Unit = {
    val inserted = dao.transact(queries.insertContracts(contracts)).unsafeRunSync()
    assert(inserted == numContracts)
  }
}

class InsertBenchmarkOracle extends InsertBenchmark with OracleBenchmarkDbConn
class InsertBenchmarkPostgres extends InsertBenchmark with PostgresBenchmarkDbConn
