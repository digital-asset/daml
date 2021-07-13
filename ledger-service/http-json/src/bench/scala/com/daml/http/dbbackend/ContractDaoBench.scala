// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import scalaz.OneAnd
import scalaz.std.list._
import cats.instances.list._
import doobie.util.log.LogHandler
import doobie.implicits._
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.{Party, TemplateId}
import com.daml.testing.oracle, oracle.{OracleAround, User}
import org.openjdk.jmh.annotations._
import scala.concurrent.ExecutionContext
import spray.json._
import spray.json.DefaultJsonProtocol._

@State(Scope.Benchmark)
class ContractDaoBench extends OracleAround {
  private implicit val logger: LogHandler = Slf4jLogHandler(getClass)
  private val tpid = TemplateId("pkg", "M", "T")
  private var surrogateTpid: SurrogateTpId = _
  private var user: User = _
  private var dao: ContractDao = _

  @Param(Array("1000"))
  var batchSize: Int = _
  @Param(Array("10"))
  var batches: Int = _

  implicit val ec: ExecutionContext = ExecutionContext.global

  private def contract(
      id: Int,
      signatory: String,
  ): DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]] = DBContract(
    contractId = s"#$id",
    templateId = surrogateTpid,
    key = JsNull,
    payload = JsObject(),
    signatories = Seq(signatory),
    observers = Seq.empty,
    agreementText = "",
  )

  private def insertBatch(dao: ContractDao, signatory: String, offset: Int) = {
    import dao.jdbcDriver._
    val contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] =
      (0 until batchSize).map { i =>
        val n = offset + i
        contract(n, signatory)
      }.toList
    val inserted = dao
      .transact(dao.jdbcDriver.queries.insertContracts[List, JsValue, JsValue](contracts))
      .unsafeRunSync()
    assert(inserted == batchSize)
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    connectToOracle()
    user = createNewRandomUser()
    val oracleDao = ContractDao("oracle.jdbc.OracleDriver", oracleJdbcUrl, user.name, user.pwd)
    dao = oracleDao

    import oracleDao.jdbcDriver

    dao.transact(ContractDao.initialize).unsafeRunSync()

    surrogateTpid = dao
      .transact(
        oracleDao.jdbcDriver.queries
          .surrogateTemplateId(tpid.packageId, tpid.moduleName, tpid.entityName)
      )
      .unsafeRunSync()

    (0 until batches - 1).foreach { batch =>
      insertBatch(dao, "Alice", batch * batchSize)
    }

    insertBatch(dao, "Bob", (batches - 1) * batchSize)
    ()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit =
    dropUser(user.name)

  @Benchmark
  def run(): Unit = {
    implicit val driver: SupportedJdbcDriver = dao.jdbcDriver
    val result = dao
      .transact(ContractDao.selectContracts(OneAnd(Party("Bob"), Set.empty), tpid, fr"1 = 1"))
      .unsafeRunSync()
    assert(result.size == batchSize)
  }
}
