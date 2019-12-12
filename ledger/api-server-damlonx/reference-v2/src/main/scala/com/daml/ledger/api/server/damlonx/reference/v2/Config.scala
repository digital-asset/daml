// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.io.File

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.indexer.IndexerStartupMode

final case class Config(
    port: Int,
    portFile: Option[File],
    archiveFiles: List[File],
    maxInboundMessageSize: Int,
    timeProvider: TimeProvider, // enables use of non-wall-clock time in tests
    jdbcUrl: String,
    tlsConfig: Option[TlsConfiguration],
    participantId: ParticipantId,
    extraParticipants: Vector[(ParticipantId, Int, String)],
    startupMode: IndexerStartupMode,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))
}

object Config {
  val DefaultMaxInboundMessageSize = 4194304

  def default: Config =
    new Config(
      port = 0,
      portFile = None,
      archiveFiles = List.empty,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      timeProvider = TimeProvider.UTC,
      jdbcUrl = "",
      tlsConfig = None,
      participantId = LedgerString.assertFromString("standalone-participant"),
      extraParticipants = Vector.empty,
      startupMode = IndexerStartupMode.MigrateAndStart,
    )
}
