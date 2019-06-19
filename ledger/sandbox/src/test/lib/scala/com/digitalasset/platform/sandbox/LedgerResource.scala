// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.persistence.{PostgresFixture, PostgresResource}
import com.digitalasset.platform.sandbox.stores.InMemoryActiveContracts
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryWithLedgerEndIncrement

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
      ledgerId: LedgerId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveContracts = InMemoryActiveContracts.empty,
      entries: ImmArray[LedgerEntryWithLedgerEndIncrement] = ImmArray.empty): Resource[Ledger] =
    LedgerResource.resource(
      () =>
        Future.successful(
          Ledger.inMemory(ledgerId, timeProvider, acs, entries)
      )
    )

  def postgres(ledgerId: LedgerId, timeProvider: TimeProvider)(
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
              InMemoryActiveContracts.empty,
              ImmArray.empty,
              128,
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
