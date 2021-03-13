// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ports.Port
import java.time.Duration

import com.daml.platform.indexer.IndexerConfig

final case class ParticipantConfig(
    mode: ParticipantRunMode,
    participantId: ParticipantId,
    // A name of the participant shard in a horizontally scaled participant.
    shardName: Option[String],
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    serverJdbcUrl: String,
    allowExistingSchemaForIndex: Boolean,
    maxCommandsInFlight: Option[Int],
    managementServiceTimeout: Duration = ParticipantConfig.defaultManagementServiceTimeout,
    indexerDatabaseConnectionPoolSize: Int =
      ParticipantConfig.defaultIndexerDatabaseConnectionPoolSize,
    apiServerDatabaseConnectionPoolSize: Int =
      ParticipantConfig.defaultApiServerDatabaseConnectionPoolSize,
    keyStateCacheSize: Int = 100000,
    contractStateCacheSize: Int = 100000,
)

object ParticipantConfig {
  def defaultIndexJdbcUrl(participantId: ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"

  val defaultManagementServiceTimeout: Duration = Duration.ofMinutes(2)

  val defaultIndexerDatabaseConnectionPoolSize = IndexerConfig.DefaultDatabaseConnectionPoolSize

  // this pool is used for all data access for the ledger api (command submission, transaction service, ...)
  val defaultApiServerDatabaseConnectionPoolSize = 16
}
