// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ports.Port

import java.time.Duration

final case class ParticipantConfig(
    participantId: ParticipantId,
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    serverJdbcUrl: String,
    allowExistingSchemaForIndex: Boolean,
    maxCommandsInFlight: Option[Int],
    managementServiceTimeout: Duration
)

object ParticipantConfig {
  def defaultIndexJdbcUrl(participantId: ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"

  val defaultManagementServiceTimeout: Duration = Duration.ofMinutes(2)
}
