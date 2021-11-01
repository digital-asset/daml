// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.services

import cats.effect.{ContextShift, IO}
import com.daml.extractor.Extractor
import com.daml.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.daml.extractor.targets.PostgreSQLTarget
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.lf.data.Ref.Party
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.ports.Port
import com.daml.testing.postgresql.{PostgresAround, PostgresAroundSuite}
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import org.scalatest._
import scalaz.OneAnd

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait ExtractorFixture extends SandboxNextFixture with PostgresAroundSuite with Types {
  self: Suite =>

  import Types._

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  protected val baseConfig: ExtractorConfig = ExtractorConfig(
    "127.0.0.1",
    ledgerPort = Port(666), // doesn't matter, will/must be overridden in the test cases
    ledgerInboundMessageSizeMax = 50 * 1024 * 1024,
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
    SnapshotEndSetting.Head,
    OneAnd(Party assertFromString "Bob", List.empty),
    Set.empty,
    TlsConfiguration(
      enabled = false,
      None,
      None,
      None,
    ),
    None,
  )

  protected def outputFormat: String = "single-table"

  protected def configureExtractor(ec: ExtractorConfig): ExtractorConfig = ec

  protected def target: PostgreSQLTarget = PostgreSQLTarget(
    connectUrl = postgresDatabase.url,
    user = postgresDatabase.userName,
    password = postgresDatabase.password,
    outputFormat = outputFormat,
    schemaPerPackage = false,
    mergeIdentical = false,
    stripPrefix = None,
  )

  protected implicit lazy val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver", // driver classname
    target.connectUrl, // connect URL (driver-specific)
    target.user,
    target.password,
  )

  protected def getTransactions: List[TransactionResult] = {
    import doobie.implicits.javasql._
    getResultList[TransactionResult](sql"SELECT * FROM transaction")
  }

  protected def getContracts: List[ContractResult] = {
    getResultList[ContractResult](sql"SELECT * FROM contract")
  }

  protected def getExercises: List[ExerciseResult] = {
    getResultList[ExerciseResult](sql"SELECT * FROM exercise")
  }

  protected def getResultList[R: Read](sql: Fragment)(implicit xa: Transactor[IO]): List[R] = {
    sql
      .query[R]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
  }

  protected def getResultOption[R: Read](sql: Fragment)(implicit xa: Transactor[IO]): Option[R] = {
    sql
      .query[R]
      .option
      .transact(xa)
      .unsafeRunSync()
  }

  protected var extractor: Extractor[PostgreSQLTarget] = _

  protected def run(): Unit = {
    val config: ExtractorConfig = configureExtractor(baseConfig.copy(ledgerPort = serverPort))

    extractor = new Extractor(config, target)()

    val res = extractor.run()

    Await.result(res, Duration.Inf)
  }

  protected def kill(): Unit = {
    val res = extractor.shutdown()

    Await.result(res, Duration.Inf)
  }
}

trait ExtractorFixtureAroundAll extends ExtractorFixture with BeforeAndAfterAll {
  self: Suite with PostgresAround =>

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    run()
  }

  override protected def afterAll(): Unit = {
    kill()
    super.afterAll()
  }
}

trait ExtractorFixtureAroundEach extends ExtractorFixture with BeforeAndAfterEach {
  self: Suite with PostgresAround =>

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    run()
  }

  override protected def afterEach(): Unit = {
    kill()
    super.afterEach()
  }
}
