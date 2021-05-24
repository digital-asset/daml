// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.ledger.participant.state.kvutils.app.ParticipantConfig.DefaultApiServerDatabaseConnectionTimeout

import java.nio.file.Path
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ports.Port

import java.time.Duration

final case class ParticipantConfig(
    mode: ParticipantRunMode,
    participantId: ParticipantId,
    // A name of the participant shard in a horizontally scaled participant.
    shardName: Option[String],
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    serverJdbcUrl: String,
    maxCommandsInFlight: Option[Int],
    managementServiceTimeout: Duration = ParticipantConfig.DefaultManagementServiceTimeout,
    indexerConfig: ParticipantIndexerConfig,
    apiServerDatabaseConnectionPoolSize: Int =
      ParticipantConfig.DefaultApiServerDatabaseConnectionPoolSize,
    apiServerDatabaseConnectionTimeout: Duration = DefaultApiServerDatabaseConnectionTimeout,
    maxContractStateCacheSize: Long = ParticipantConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = ParticipantConfig.DefaultMaxContractKeyStateCacheSize,
)

object ParticipantConfig {
  def defaultIndexJdbcUrl(participantId: ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"

  val DefaultManagementServiceTimeout: Duration = Duration.ofMinutes(2)
  val DefaultApiServerDatabaseConnectionTimeout: Duration = Duration.ofMillis(250)

  // this pool is used for all data access for the ledger api (command submission, transaction service, ...)
  val DefaultApiServerDatabaseConnectionPoolSize = 16

  val DefaultMaxContractStateCacheSize: Long = 100000
  val DefaultMaxContractKeyStateCacheSize: Long = 100000
}
