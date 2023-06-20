// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.domain.{Party, ContractTypeId}
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.nonempty.NonEmpty
import doobie.implicits._
import org.openjdk.jmh.annotations._

trait QueryBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("1", "10"))
  var extraParties: Int = _

  @Param(Array("1", "10"))
  var extraTemplates: Int = _

  private val tpid = ContractTypeId.Template("-pkg-", "M", "T")
  private var surrogateTpid: SurrogateTpId = _
  val party = "Alice"

  override def trialSetupPostInitialize(): Unit = {
    surrogateTpid = insertTemplate(tpid)

    val surrogateTpids = surrogateTpid :: (0 until extraTemplates)
      .map(i => insertTemplate(ContractTypeId.Template("-pkg-", "M", s"T$i")))
      .toList

    val parties: List[String] = party :: (0 until extraParties).map(i => s"p$i").toList

    var offset = 0
    parties.foreach { p =>
      surrogateTpids.foreach { t =>
        insertBatch(p, t, offset)
        offset += batchSize
      }
    }
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime))
  @Fork(1)
  @Warmup(iterations = 3)
  @Measurement(iterations = 10)
  def run(): Unit = {
    implicit val driver: SupportedJdbcDriver.TC = dao.jdbcDriver
    val result = instanceUUIDLogCtx(implicit lc =>
      dao
        .transact(
          ContractDao.selectContracts(NonEmpty(Set, Party(party)), tpid, fr"1 = 1")
        )
        .unsafeRunSync()
    )
    assert(result.size == batchSize)
  }

}

class QueryBenchmarkOracle extends QueryBenchmark with OracleBenchmarkDbConn
class QueryBenchmarkPostgres extends QueryBenchmark with PostgresBenchmarkDbConn
