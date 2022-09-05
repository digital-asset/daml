// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.lf.data.Ref
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.IndexerConfig
import com.daml.ports.Port

import java.nio.file.Path
import scala.concurrent.duration._

final case class CliParticipantConfig(
    participantId: Ref.ParticipantId,
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    serverJdbcUrl: String,
    managementServiceTimeout: Duration = CliParticipantConfig.DefaultManagementServiceTimeout,
    indexerConfig: IndexerConfig,
    apiServerDatabaseConnectionPoolSize: Int =
      CliParticipantConfig.DefaultApiServerDatabaseConnectionPoolSize,
    apiServerDatabaseConnectionTimeout: Duration =
      CliParticipantConfig.DefaultApiServerDatabaseConnectionTimeout,
    maxContractStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractKeyStateCacheSize,
)

object CliParticipantConfig {
  val DefaultManagementServiceTimeout: FiniteDuration = 2.minutes
  val DefaultApiServerDatabaseConnectionTimeout: Duration = 250.millis

  // this pool is used for all data access for the ledger api (command submission, transaction service, ...)
  val DefaultApiServerDatabaseConnectionPoolSize = 16
}
