// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.lf.data.Ref
import com.daml.platform.configuration.IndexConfiguration
import com.daml.platform.indexer.IndexerConfig
import com.daml.ports.Port

import java.nio.file.Path
import scala.concurrent.duration._

final case class CliParticipantConfig(
    mode: ParticipantRunMode,
    participantId: Ref.ParticipantId,
    // A name of the participant shard in a horizontally scaled participant.
    shardName: Option[String],
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
    maxContractStateCacheSize: Long = IndexConfiguration.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexConfiguration.DefaultMaxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize: Long =
      IndexConfiguration.DefaultMaxTransactionsInMemoryFanOutBufferSize,
)

object CliParticipantConfig {
  def defaultIndexJdbcUrl(participantId: Ref.ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"

  val DefaultManagementServiceTimeout: FiniteDuration = 2.minutes
  val DefaultApiServerDatabaseConnectionTimeout: Duration = 250.millis

  // this pool is used for all data access for the ledger api (command submission, transaction service, ...)
  val DefaultApiServerDatabaseConnectionPoolSize = 16
}
