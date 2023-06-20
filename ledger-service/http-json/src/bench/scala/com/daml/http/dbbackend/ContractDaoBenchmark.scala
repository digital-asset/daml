// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.instances.list._
import com.daml.dbutils
import com.daml.dbutils.ConnectionPool
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.OracleQueries.DisableContractPayloadIndexing
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.ContractTypeId
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.testing.oracle.OracleAround
import com.daml.testing.oracle.OracleAround.RichOracleUser
import com.daml.testing.postgresql.{PostgresAround, PostgresDatabase}
import doobie.util.log.LogHandler
import org.openjdk.jmh.annotations._
import scalaz.std.list._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext

@State(Scope.Benchmark)
abstract class ContractDaoBenchmark {
  self: BenchmarkDbConnection =>

  protected var dao: ContractDao = _
  protected[this] final def queries = dao.jdbcDriver.q.queries

  protected implicit val ec: ExecutionContext = ExecutionContext.global
  protected implicit val logger: LogHandler = Slf4jLogHandler(getClass)
  protected implicit val metrics: HttpJsonApiMetrics = HttpJsonApiMetrics.ForTesting

  @Param(Array("1000"))
  var batchSize: Int = _

  @Setup(Level.Trial)
  def trialSetup(): Unit = {
    connectToDb()
    val cfg = createDbJdbcConfig
    val cDao = ContractDao(cfg)
    dao = cDao

    import cDao.jdbcDriver

    dao.transact(ContractDao.initialize).unsafeRunSync()

    trialSetupPostInitialize()
  }

  def trialSetupPostInitialize(): Unit

  @TearDown(Level.Trial)
  def trialTearDown(): Unit = {
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

  protected def insertTemplate(tpid: ContractTypeId.RequiredPkg): SurrogateTpId = {
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

trait OracleBenchmarkDbConn extends BenchmarkDbConnection {

  private var user: RichOracleUser = _
  private val disableContractPayloadIndexing = false

  override def connectToDb() = user = OracleAround.createNewUniqueRandomUser()

  override def createDbJdbcConfig: JdbcConfig = {
    val cfg = JdbcConfig(
      dbutils.JdbcConfig(
        driver = "oracle.jdbc.OracleDriver",
        url = user.jdbcUrlWithoutCredentials,
        user = user.oracleUser.name,
        password = user.oracleUser.pwd,
        poolSize = ConnectionPool.PoolSize.Integration,
      ),
      startMode = DbStartupMode.CreateOnly,
      backendSpecificConf =
        if (disableContractPayloadIndexing) Map(DisableContractPayloadIndexing -> "true")
        else Map.empty,
    )
    cfg
  }

  override def cleanup(): Unit = user.drop()
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
      startMode = DbStartupMode.CreateOnly,
    )
  }

  override def cleanup(): Unit = {
    dropDatabase(database)
    disconnectFromPostgresqlServer()
  }
}
