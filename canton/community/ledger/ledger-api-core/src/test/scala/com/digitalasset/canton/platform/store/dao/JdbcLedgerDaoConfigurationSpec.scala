// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import akka.stream.scaladsl.Sink
import com.digitalasset.canton.platform.store.dao.*
import com.digitalasset.canton.platform.store.entries.ConfigurationEntry
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

trait JdbcLedgerDaoConfigurationSpec {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite with OptionValues =>

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
      endingOffset.lastOffset should be > startingOffset.lastOffset
    }
  }

  it should "refuse to persist invalid configuration entry" in {
    val startExclusive = nextOffset()
    val offset0 = nextOffset()
    val offsetString0 = offset0.toLong
    for {
      config0 <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).getOrElse(defaultConfig))

      // Store a new configuration with a known submission id
      submissionId = s"refuse-config-$offsetString0"
      resp0 <- storeConfigurationEntry(
        offset0,
        submissionId,
        config0.copy(generation = config0.generation + 1),
      )
      config1 <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).value)

      // Duplicate submission is accepted
      offset1 = nextOffset()
      resp1 <- storeConfigurationEntry(
        offset1,
        submissionId,
        config1.copy(generation = config1.generation + 1),
      )
      config2 <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).value)

      // Submission with unique submissionId and correct generation is accepted.
      offset2 = nextOffset()
      offsetString2 = offset2.toLong
      lastConfig = config1.copy(generation = config1.generation + 2)
      resp3 <- storeConfigurationEntry(offset2, s"refuse-config-$offsetString2", lastConfig)
      lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).value)

      entries <- ledgerDao.getConfigurationEntries(startExclusive, offset2).runWith(Sink.seq)
    } yield {
      resp0 shouldEqual PersistenceResponse.Ok
      resp1 shouldEqual PersistenceResponse.Ok
      resp3 shouldEqual PersistenceResponse.Ok
      lastConfig shouldEqual lastConfigActual
      entries.toList shouldEqual List(
        offset0 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString0", config1),
        /* offset1 is duplicate */
        offset1 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString0", config2),
        offset2 -> ConfigurationEntry.Accepted(s"refuse-config-$offsetString2", lastConfig),
      )
    }
  }
}
