// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.domain.{Party, TemplateId}
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.scalautil.Statement.discard
import com.daml.scalautil.nonempty.NonEmpty
import doobie.implicits._
import org.openjdk.jmh.annotations._

import scala.collection.compat._

trait QueryBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("1", "5", "9"))
  var extraParties: Int = _

  @Param(Array("1", "5", "9"))
  var extraTemplates: Int = _

  private val tpid = TemplateId("-pkg-", "M", "T")
  private var surrogateTpid: SurrogateTpId = _
  val party = "Alice"

  @Setup(Level.Trial)
  override def setup(): Unit = {
    super.setup()
    surrogateTpid = insertTemplate(tpid)

    val surrogateTpids = surrogateTpid :: (0 until extraTemplates)
      .map(i => insertTemplate(TemplateId("-pkg-", "M", s"T$i")))
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
  def run(): Unit = {
    implicit val driver: SupportedJdbcDriver.TC = dao.jdbcDriver
    val result = instanceUUIDLogCtx(implicit lc =>
      dao
        .transact(
          ContractDao.selectContracts(NonEmpty.pour(Party(party)) into Set, tpid, fr"1 = 1")
        )
        .unsafeRunSync()
    )
    assert(result.size == batchSize)
  }

  discard(IterableOnce) // only needed for scala 2.12
}

class QueryBenchmarkOracle extends QueryBenchmark with OracleBenchmarkDbConn
class QueryBenchmarkPostgres extends QueryBenchmark with PostgresBenchmarkDbConn
