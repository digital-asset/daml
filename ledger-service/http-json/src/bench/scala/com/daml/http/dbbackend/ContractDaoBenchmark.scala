// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import doobie.util.log.LogHandler
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.TemplateId
import com.daml.testing.oracle, oracle.{OracleAround, User}
import org.openjdk.jmh.annotations._
import scala.concurrent.ExecutionContext
import scalaz.std.list._
import spray.json._
import spray.json.DefaultJsonProtocol._

@State(Scope.Benchmark)
abstract class ContractDaoBenchmark extends OracleAround {

  private var user: User = _

  protected var dao: ContractDao = _

  protected implicit val ec: ExecutionContext = ExecutionContext.global
  protected implicit val logger: LogHandler = Slf4jLogHandler(getClass)

  @Param(Array("1000"))
  var batchSize: Int = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    connectToOracle()
    user = createNewRandomUser()
    val cfg = new JdbcConfig("oracle.jdbc.OracleDriver", oracleJdbcUrl, user.name, user.pwd)
    val oracleDao = ContractDao(cfg)
    dao = oracleDao

    import oracleDao.jdbcDriver

    dao.transact(ContractDao.initialize).unsafeRunSync()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    dao.close()
    dropUser(user.name)
  }

  protected def contract(
      id: Int,
      signatory: String,
      tpid: SurrogateTpId,
      payload: JsObject = JsObject(),
  ): DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]] = DBContract(
    contractId = s"#$id",
    templateId = tpid,
    key = JsNull,
    keyHash = "",
    payload = payload,
    signatories = Seq(signatory),
    observers = Seq.empty,
    agreementText = "",
  )

  protected def insertTemplate(tpid: TemplateId.RequiredPkg): SurrogateTpId = {
    dao
      .transact(
        dao.jdbcDriver.queries
          .surrogateTemplateId(tpid.packageId, tpid.moduleName, tpid.entityName)
      )
      .unsafeRunSync()
  }

  protected def insertBatch(
      signatory: String,
      tpid: SurrogateTpId,
      offset: Int,
      payload: JsObject = JsObject(),
  ) = {
    val driver = dao.jdbcDriver
    import driver._
    val contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] =
      (0 until batchSize).map { i =>
        val n = offset + i
        contract(n, signatory, tpid, payload)
      }.toList
    val inserted = dao
      .transact(driver.queries.insertContracts[List, JsValue, JsValue](contracts))
      .unsafeRunSync()
    assert(inserted == batchSize)
  }

}
