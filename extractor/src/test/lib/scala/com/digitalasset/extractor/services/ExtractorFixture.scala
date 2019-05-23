// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.services

import com.digitalasset.extractor.Extractor
import com.digitalasset.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.digitalasset.extractor.targets.PostgreSQLTarget
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.sandbox.persistence.PostgresAround
import com.digitalasset.platform.sandbox.services.SandboxFixture

import scalaz.OneAnd
import cats.effect.{ContextShift, IO}
import doobie._
import doobie.implicits._

import org.scalatest._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait ExtractorFixture extends SandboxFixture with PostgresAround with Types {
  self: Suite =>

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  protected val baseConfig = ExtractorConfig(
    "127.0.0.1",
    666, // doesn't matter, will/must be overriden in the test cases
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
    SnapshotEndSetting.Head,
    OneAnd("Bob", List.empty),
    TlsConfiguration(
      enabled = false,
      None,
      None,
      None
    )
  )

  protected lazy val target: PostgreSQLTarget = PostgreSQLTarget(
    connectUrl = postgresFixture.jdbcUrl,
    user = "test",
    password = "",
    outputFormat = "combined",
    schemaPerPackage = false,
    mergeIdentical = false,
    stripPrefix = None
  )

  protected implicit lazy val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver", // driver classname
    target.connectUrl, // connect URL (driver-specific)
    target.user,
    target.password
  )

  protected def getTransactions: List[TransactionResult] = {
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
    val config: ExtractorConfig = baseConfig.copy(ledgerPort = getSandboxPort)

    extractor = new Extractor(config, target)

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
