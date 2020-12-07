// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.daml.platform.store.entries.ConfigurationEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

trait JdbcLedgerDaoConfigurationSpec { this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (configuration)"

  it should "be able to persist and load configuration" in {
    val offset = nextOffset()
    val offsetString = offset.toLong
    for {
      startingOffset <- ledgerDao.lookupLedgerEnd()
      startingConfig <- ledgerDao.lookupLedgerConfiguration()

      response <- ledgerDao.storeConfigurationEntry(
        offset,
        Instant.EPOCH,
        s"submission-$offsetString",
        defaultConfig,
        None,
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
      response <- ledgerDao.storeConfigurationEntry(
        offset,
        Instant.EPOCH,
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

  it should "refuse to persist invalid configuration entry" in {
    val startExclusive = nextOffset()
    val offset0 = nextOffset()
    val offsetString0 = offset0.toLong
    for {
      config <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).getOrElse(defaultConfig))

      // Store a new configuration with a known submission id
      submissionId = s"refuse-config-$offsetString0"
      resp0 <- ledgerDao.storeConfigurationEntry(
        offset0,
        Instant.EPOCH,
        submissionId,
        config.copy(generation = config.generation + 1),
        None,
      )
      newConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      // Submission with duplicate submissionId is rejected
      offset1 = nextOffset()
      resp1 <- ledgerDao.storeConfigurationEntry(
        offset1,
        Instant.EPOCH,
        submissionId,
        newConfig.copy(generation = config.generation + 1),
        None,
      )

      // Submission with mismatching generation is rejected
      offset2 = nextOffset()
      offsetString2 = offset2.toLong
      resp2 <- ledgerDao.storeConfigurationEntry(
        offset2,
        Instant.EPOCH,
        s"refuse-config-$offsetString2",
        config,
        None,
      )

      // Submission with unique submissionId and correct generation is accepted.
      offset3 = nextOffset()
      offsetString3 = offset3.toLong
      lastConfig = newConfig.copy(generation = newConfig.generation + 1)
      resp3 <- ledgerDao.storeConfigurationEntry(
        offset3,
        Instant.EPOCH,
        s"refuse-config-$offsetString3",
        lastConfig,
        None,
      )
      lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      entries <- ledgerDao.getConfigurationEntries(startExclusive, offset3).runWith(Sink.seq)
    } yield {
      resp0 shouldEqual PersistenceResponse.Ok
      resp1 shouldEqual PersistenceResponse.Duplicate
      resp2 shouldEqual PersistenceResponse.Ok
      resp3 shouldEqual PersistenceResponse.Ok
      lastConfig shouldEqual lastConfigActual
      entries.toList shouldEqual List(
        offset0 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString0", newConfig),
        /* offset1 is duplicate */
        offset2 -> ConfigurationEntry.Rejected(
          s"refuse-config-${offset2.toLong}",
          "Generation mismatch: expected=2, actual=0",
          config,
        ),
        offset3 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString3", lastConfig)
      )
    }
  }

}
