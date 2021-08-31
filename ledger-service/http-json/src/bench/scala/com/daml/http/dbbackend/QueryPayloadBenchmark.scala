// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.domain.{Party, TemplateId}
import com.daml.http.query.ValuePredicate
import org.openjdk.jmh.annotations._
import scalaz.OneAnd
import spray.json._

class QueryPayloadBenchmark extends ContractDaoBenchmark {
  @Param(Array("1", "10", "100"))
  var extraParties: Int = _

  @Param(Array("1", "100", "100"))
  var extraPayloadValues: Int = _

  private val tpid = TemplateId("-pkg-", "M", "T")
  private var surrogateTpid: SurrogateTpId = _
  val party = "Alice"

  private[this] val dummyPackageId = Ref.PackageId.assertFromString("dummy-package-id")
  private[this] val dummyId = Ref.Identifier(
    dummyPackageId,
    Ref.QualifiedName.assertFromString("Foo:Bar"),
  )
  private[this] val dummyTypeCon = iface.TypeCon(iface.TypeConName(dummyId), ImmArraySeq.empty)

  val predicate = ValuePredicate.fromJsObject(
    Map("v" -> JsNumber(0)),
    dummyTypeCon,
    Map(
      dummyId -> iface.DefDataType(
        ImmArraySeq.empty,
        iface.Record(
          ImmArraySeq(
            (Ref.Name.assertFromString("v"), iface.TypePrim(iface.PrimType.Int64, ImmArraySeq()))
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
    val result = dao
      .transact(ContractDao.selectContracts(OneAnd(Party(party), Set.empty), tpid, whereClause))
      .unsafeRunSync()
    assert(result.size == batchSize)
  }
}
