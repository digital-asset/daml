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
import scalaz.std.vector._

import scala.collection.compat._

trait QueryBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("5000"))
  var tableSize: Int = _

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
    def tpidCycle = Iterator continually surrogateTpids flatMap (_.iterator)

    val parties: List[String] = party :: (0 until extraParties).map(i => s"p$i").toList
    val contractsPerParty = tableSize / (extraParties + 1)

    var offset = 0

    parties.iterator
      .flatMap(p => tpidCycle.take(contractsPerParty).map((p, _)))
      .grouped(batchSize)
      .foreach { batch =>
        val inserted =
          insertContracts(batch.view.zipWithIndex.map { case ((p, t), i) =>
            contract(offset + i, p, t)
          }.toVector)
        assert(inserted == batch.size)
        offset += batch.size
      }
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime))
  def run(): Unit = {
    implicit val driver: SupportedJdbcDriver.TC = dao.jdbcDriver
    val /*result*/ _ = instanceUUIDLogCtx(implicit lc =>
      dao
        .transact(
          ContractDao.selectContracts(NonEmpty.pour(Party(party)) into Set, tpid, fr"1 = 1")
        )
        .unsafeRunSync()
    )
    // assert(result.size == batchSize)
  }

  discard(IterableOnce) // only needed for scala 2.12
}

class QueryBenchmarkOracle extends QueryBenchmark with OracleBenchmarkDbConn
class QueryBenchmarkPostgres extends QueryBenchmark with PostgresBenchmarkDbConn
