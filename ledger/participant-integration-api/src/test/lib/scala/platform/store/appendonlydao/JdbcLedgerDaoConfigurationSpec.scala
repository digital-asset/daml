// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.time.Instant

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.platform.indexer.{CurrentOffset, IncrementalOffsetStep}
import com.daml.platform.store.dao.PersistenceResponse
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

  it should "refuse to persist invalid configuration entry" in {
    val startExclusive = nextOffset()
    val offset0 = nextOffset()
    for {
      config <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).getOrElse(defaultConfig))

      // Store a new configuration with a known submission id
      submissionId = s"refuse-config-${offset0.toLong}"
      resp0 <- storeConfigurationEntry(
        offset0,
        submissionId,
        config.copy(generation = config.generation + 1),
      )
      newConfig <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      resp1 <- storeConfigurationEntry(
        nextOffset(),
        submissionId,
        newConfig.copy(generation = config.generation + 1),
      )

      // Submission with unique submissionId and correct generation is accepted.
      offset2 = nextOffset()
      lastConfig = newConfig.copy(generation = newConfig.generation + 1)
      resp2 <- storeConfigurationEntry(offset2, s"refuse-config-${offset2.toLong}", lastConfig)
      lastConfigActual <- ledgerDao.lookupLedgerConfiguration().map(_.map(_._2).get)

      entries <- ledgerDao.getConfigurationEntries(startExclusive, offset2).runWith(Sink.seq)
    } yield {
      resp0 shouldEqual PersistenceResponse.Ok
      resp1 shouldEqual PersistenceResponse.Duplicate
      resp2 shouldEqual PersistenceResponse.Ok
      lastConfig shouldEqual lastConfigActual
      entries.toList shouldEqual List(
        offset0 -> ConfigurationEntry.Accepted(s"refuse-config-${offset0.toLong}", newConfig),
        /* offset1 is duplicate */
        offset2 -> ConfigurationEntry.Accepted(s"refuse-config-${offset2.toLong}", lastConfig),
      )
    }
  }

  // TODO do we need it?
//  it should "fail trying to store configuration with non-incremental offsets" in {
//    recoverToSucceededIf[LedgerEndUpdateError](
//      storeConfigurationEntry(
//        nextOffset(),
//        s"submission-invalid-offsets",
//        defaultConfig,
//        maybePreviousOffset = Some(nextOffset()),
//      )
//    )
//  }

  private def storeConfigurationEntry(
      offset: Offset,
      submissionId: String,
      lastConfig: Configuration,
      rejectionReason: Option[String] = None,
      maybePreviousOffset: Option[Offset] = Option.empty,
  ) =
    ledgerDao
      .storeConfigurationEntry(
        offsetStep = maybePreviousOffset
          .orElse(previousOffset.get())
          .map(IncrementalOffsetStep(_, offset))
          .getOrElse(CurrentOffset(offset)),
        Instant.EPOCH,
        submissionId,
        lastConfig,
        rejectionReason,
      )
      .map { r =>
        previousOffset.set(Some(offset))
        r
      }
}
