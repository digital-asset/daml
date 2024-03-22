// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.configuration.Configuration
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.InitialLedgerConfiguration
import com.daml.platform.services.time.TimeProviderType

import java.time.{Duration, Instant}

class ConfigAdaptor {
  def initialLedgerConfig(
      maxDeduplicationDuration: Option[Duration]
  ): InitialLedgerConfiguration = {
    val conf = Configuration.reasonableInitialConfiguration
    InitialLedgerConfiguration(
      maxDeduplicationDuration = maxDeduplicationDuration.getOrElse(
        conf.maxDeduplicationDuration
      ),
      avgTransactionLatency = conf.timeModel.avgTransactionLatency,
      minSkew = conf.timeModel.minSkew,
      maxSkew = conf.timeModel.maxSkew,
      // If a new index database is added to an already existing ledger,
      // a zero delay will likely produce a "configuration rejected" ledger entry,
      // because at startup the indexer hasn't ingested any configuration change yet.
      // Override this setting for distributed ledgers where you want to avoid these superfluous entries.
      delayBeforeSubmitting = Duration.ZERO,
    )
  }

  def timeServiceBackend(config: ApiServerConfig): Option[TimeServiceBackend] =
    config.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  def authService(participantConfig: ParticipantConfig): AuthService =
    participantConfig.authentication.create()
}
