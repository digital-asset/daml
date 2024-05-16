// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.ContractTypeId
import com.daml.lf.data.Ref.PackageId
import org.openjdk.jmh.annotations._
import scalaz.std.list._
import spray.json._
import spray.json.DefaultJsonProtocol._

trait InsertBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("1", "100"))
  var batches: Int = _

  @Param(Array("1000"))
  var numContracts: Int = _

  private var contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] = _

  private var contractCids: Set[String] = _

  private var tpid: SurrogateTpId = _

  override def trialSetupPostInitialize(): Unit = {
    tpid = insertTemplate(ContractTypeId.Template(PackageId.assertFromString("-pkg-"), "M", "T"))
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
  @Fork(1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  def run(): Unit = {
    val inserted = dao.transact(queries.insertContracts(contracts)).unsafeRunSync()
    assert(inserted == numContracts)
  }
}

class InsertBenchmarkOracle extends InsertBenchmark with OracleBenchmarkDbConn
class InsertBenchmarkPostgres extends InsertBenchmark with PostgresBenchmarkDbConn
