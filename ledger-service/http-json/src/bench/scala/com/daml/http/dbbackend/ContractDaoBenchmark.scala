// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import com.codahale.metrics.MetricRegistry
import doobie.util.log.LogHandler
import com.daml.dbutils
import com.daml.dbutils.ConnectionPool
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.OracleQueries.DisableContractPayloadIndexing
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.TemplateId
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.metrics.Metrics
import com.daml.testing.oracle
import com.daml.testing.postgresql.{PostgresAround, PostgresDatabase}
import oracle.{OracleAround, User}
import org.openjdk.jmh.annotations._

import scala.concurrent.ExecutionContext
import scalaz.std.list._
import spray.json._
import spray.json.DefaultJsonProtocol._

@State(Scope.Benchmark)
abstract class ContractDaoBenchmark extends OracleAround {
  self: BenchmarkDbConnection =>

  protected var dao: ContractDao = _
  protected[this] final def queries = dao.jdbcDriver.q.queries

  protected implicit val ec: ExecutionContext = ExecutionContext.global
  protected implicit val logger: LogHandler = Slf4jLogHandler(getClass)
  protected implicit val metrics: Metrics = new Metrics(new MetricRegistry())

  @Param(Array("1000"))
  var batchSize: Int = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    connectToDb()
    val cfg = createDbJdbcConfig
    val cDao = ContractDao(cfg)
    dao = cDao

    import cDao.jdbcDriver

    dao.transact(ContractDao.initialize).unsafeRunSync()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    dao.close()
    cleanup()
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
    keyHash = None,
    payload = payload,
    signatories = Seq(signatory),
    observers = Seq.empty,
    agreementText = "",
  )

  protected def insertTemplate(tpid: TemplateId.RequiredPkg): SurrogateTpId = {
    instanceUUIDLogCtx(implicit lc =>
      dao
        .transact(
          queries
            .surrogateTemplateId(tpid.packageId, tpid.moduleName, tpid.entityName)
        )
        .unsafeRunSync()
    )
  }

  protected def insertBatch(
      signatory: String,
      tpid: SurrogateTpId,
      offset: Int,
      payload: JsObject = JsObject(),
  ) = {
    val contracts: List[DBContract[SurrogateTpId, JsValue, JsValue, Seq[String]]] =
      (0 until batchSize).map { i =>
        val n = offset + i
        contract(n, signatory, tpid, payload)
      }.toList
    val inserted = dao
      .transact(queries.insertContracts[List, JsValue, JsValue](contracts))
      .unsafeRunSync()
    assert(inserted == batchSize)
  }
}

trait BenchmarkDbConnection {
  def connectToDb(): Unit
  def createDbJdbcConfig: JdbcConfig
  def cleanup(): Unit
}

trait OracleBenchmarkDbConn extends BenchmarkDbConnection with OracleAround {

  private var user: User = _
  private val disableContractPayloadIndexing = false

  override def connectToDb() = {
    connectToOracle()
  }

  override def createDbJdbcConfig: JdbcConfig = {
    user = createNewRandomUser()
    val cfg = JdbcConfig(
      dbutils.JdbcConfig(
        driver = "oracle.jdbc.OracleDriver",
        url = oracleJdbcUrl,
        user = user.name,
        password = user.pwd,
        poolSize = ConnectionPool.PoolSize.Integration,
      ),
      dbStartupMode = DbStartupMode.CreateOnly,
      backendSpecificConf =
        if (disableContractPayloadIndexing) Map(DisableContractPayloadIndexing -> "true")
        else Map.empty,
    )
    cfg
  }

  override def cleanup(): Unit = {
    dropUser(user.name)
  }
}

trait PostgresBenchmarkDbConn extends BenchmarkDbConnection with PostgresAround {

  @volatile
  private var database: PostgresDatabase = _

  override def connectToDb() = {
    connectToPostgresqlServer()
    database = createNewRandomDatabase()
  }

  override def createDbJdbcConfig: JdbcConfig = {
    JdbcConfig(
      dbutils.JdbcConfig(
        driver = "org.postgresql.Driver",
        url = database.url,
        user = database.userName,
        password = database.password,
        poolSize = ConnectionPool.PoolSize.Integration,
      ),
      dbStartupMode = DbStartupMode.CreateOnly,
    )
  }

  override def cleanup(): Unit = {
    dropDatabase(database)
    disconnectFromPostgresqlServer()
  }
}
