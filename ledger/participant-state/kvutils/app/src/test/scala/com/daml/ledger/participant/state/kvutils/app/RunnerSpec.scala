// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.testing.postgresql.{PostgresAround, PostgresAroundAll}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar._
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future
import scala.util.control.NonFatal

final class RunnerSpec
    extends AsyncFlatSpec
    with Matchers
    with PostgresAround
    with PostgresAroundAll {

  private val MockedLedgerFactory = mock[LedgerFactory[ReadWriteService, Unit]]
  when(MockedLedgerFactory.defaultExtraConfig).thenReturn(())
  when(MockedLedgerFactory.manipulateConfig(any[Config[Unit]])).thenAnswer[Config[Unit]](identity)

  private val DumpIndexMetadataCommand = "dump-index-metadata"

  behavior of "Runner"

  it should "fail if a participant is not provided in run mode" in {

    val runner = new Runner("Test", MockedLedgerFactory).owner(Seq.empty)
    runner.use(_ => Future.unit).map(_ => fail).recover { case NonFatal(_) => succeed }

  }

  it should "fail if a participant is not provided when dumping the index metadata" in {

    val runner = new Runner("Test", MockedLedgerFactory).owner(Seq(DumpIndexMetadataCommand))
    runner.use(_ => Future.unit).map(_ => fail).recover { case NonFatal(_) => succeed }

  }

  it should "succeed if a participant is provided when dumping the index metadata" in {

    val runner = new Runner("Test", MockedLedgerFactory)
      .owner(Seq(DumpIndexMetadataCommand, postgresDatabase.url))
    runner.use(_ => Future.successful(succeed))

  }

  it should "succeed if more than one participant is provided when dumping the index metadata" in {

    val runner = new Runner("Test", MockedLedgerFactory)
      .owner(Seq(DumpIndexMetadataCommand, postgresDatabase.url, postgresDatabase.url))
    runner.use(_ => Future.successful(succeed))

  }

}
