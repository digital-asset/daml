// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.Sink
import com.daml.platform.store.appendonlydao._
import com.daml.platform.store.entries.ConfigurationEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

trait JdbcLedgerDaoConfigurationSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (configuration)"

  it should "be able to persist and load configuration" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      startingConfig <- ledgerDao.lookupLedgerConfiguration()

      response <- storeConfigurationEntry(
        offset,
        s"submission-$offsetString",
        defaultConfig,
      )
      optStoredConfig <- ledgerDao.lookupLedgerConfiguration()
      endingOffset <- ledgerDao.lookupLedgerEnd()
    } yield {
      response shouldEqual PersistenceResponse.Ok
      startingConfig shouldEqual None
      optStoredConfig.map(_._2) shouldEqual Some(defaultConfig)
      endingOffset should be > startingOffset
    }
  }

  it should "be able to persist configuration rejection" in {
    val startExclusive = nextOffset()
    val offset = nextOffset()
    val offsetString = offset.toLong
    for {
      startingConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2))
      proposedConfig = startingConfig.getOrElse(defaultConfig)
      response <- storeConfigurationEntry(
        offset,
        s"config-rejection-$offsetString",
        proposedConfig,
        Some("bad config"),
      )
      storedConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2))
      entries <- ledgerDao
        .getConfigurationEntries(startExclusive, offset)
        .runWith(Sink.seq)

    } yield {
      response shouldEqual PersistenceResponse.Ok
      startingConfig shouldEqual storedConfig
      entries shouldEqual List(
        offset -> ConfigurationEntry
          .Rejected(s"config-rejection-$offsetString", "bad config", proposedConfig)
      )
    }
  }
}
