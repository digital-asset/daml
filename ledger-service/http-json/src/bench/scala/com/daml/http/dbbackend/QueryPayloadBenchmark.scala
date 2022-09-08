// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.typesig
import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.domain.{Party, ContractTypeId}
import com.daml.http.query.ValuePredicate
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.nonempty.NonEmpty
import org.openjdk.jmh.annotations._
import spray.json._

trait QueryPayloadBenchmark extends ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  @Param(Array("1", "10", "100"))
  var extraParties: Int = _

  @Param(Array("1", "100", "100"))
  var extraPayloadValues: Int = _

  private val tpid = ContractTypeId.Template("-pkg-", "M", "T")
  private var surrogateTpid: SurrogateTpId = _
  val party = "Alice"

  private[this] val dummyPackageId = Ref.PackageId.assertFromString("dummy-package-id")
  private[this] val dummyId = Ref.Identifier(
    dummyPackageId,
    Ref.QualifiedName.assertFromString("Foo:Bar"),
  )
  private[this] val dummyTypeCon = typesig.TypeCon(typesig.TypeConName(dummyId), ImmArraySeq.empty)

  val predicate = ValuePredicate.fromJsObject(
    Map("v" -> JsNumber(0)),
    dummyTypeCon,
    Map(
      dummyId -> typesig.DefDataType(
        ImmArraySeq.empty,
        typesig.Record(
          ImmArraySeq(
            (
              Ref.Name.assertFromString("v"),
              typesig.TypePrim(typesig.PrimType.Int64, ImmArraySeq()),
            )
          )
        ),
      )
    ).lift,
  )

  def whereClause = predicate.toSqlWhereClause(dao.jdbcDriver)

  @Setup(Level.Trial)
  override def setup(): Unit = {
    super.setup()
    surrogateTpid = insertTemplate(tpid)

    val parties: List[String] = party :: (0 until extraParties).map(i => s"p$i").toList

    var offset = 0
    parties.foreach { p =>
      (0 until extraPayloadValues).foreach { v =>
        insertBatch(p, surrogateTpid, offset, JsObject("v" -> JsNumber(v)))
        offset += batchSize
      }
    }
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime))
  def run(): Unit = {
    implicit val sjd: SupportedJdbcDriver.TC = dao.jdbcDriver
    val result = instanceUUIDLogCtx(implicit lc =>
      dao
        .transact(
          ContractDao.selectContracts(NonEmpty(Set, Party(party)), tpid, whereClause)
        )
        .unsafeRunSync()
    )
    assert(result.size == batchSize)
  }

}

class QueryPayloadBenchmarkOracle extends QueryPayloadBenchmark with OracleBenchmarkDbConn
class QueryPayloadBenchmarkPostgres extends QueryPayloadBenchmark with PostgresBenchmarkDbConn
