// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.persistence.{PostgresFixture, PostgresResource}
import com.digitalasset.platform.sandbox.stores.ActiveContractsInMemory
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object LedgerResource {
  def resource(ledgerFactory: () => Future[Ledger]): Resource[Ledger] = new Resource[Ledger] {
    @volatile
    var ledger: Ledger = _

    override def value: Ledger = ledger

    override def setup(): Unit = ledger = Await.result(ledgerFactory(), 30.seconds)

    override def close(): Unit = ledger.close()
  }

  def inMemory(
      ledgerId: String,
      timeProvider: TimeProvider,
      acs: ActiveContractsInMemory = ActiveContractsInMemory.empty,
      entries: Seq[LedgerEntry] = Nil): Resource[Ledger] =
    LedgerResource.resource(
      () =>
        Future.successful(
          Ledger.inMemory(ledgerId, timeProvider, acs, entries)
      )
    )

  def postgres(ledgerId: String, timeProvider: TimeProvider)(
      implicit mat: Materializer,
      mm: MetricsManager) = {
    new Resource[Ledger] {
      @volatile
      private var postgres: Resource[PostgresFixture] = null

      @volatile
      private var ledger: Resource[Ledger] = null

      override def value(): Ledger = ledger.value

      override def setup(): Unit = {
        postgres = PostgresResource()
        postgres.setup()

        ledger = LedgerResource.resource(
          () =>
            Ledger.postgres(
              postgres.value.jdbcUrl,
              ledgerId,
              timeProvider,
              Nil,
              2,
              SqlStartMode.AlwaysReset))
        ledger.setup()
      }

      override def close(): Unit = {
        ledger.close()
        postgres.close()
        postgres = null
        ledger = null
      }
    }
  }

}
