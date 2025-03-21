// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins.toxiproxy

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.Port

sealed trait ProxyInstanceConfig {
  val name: String
  val upstreamHost: String
  val upstreamPort: Port
  val from: ProxyConfig
}

final case class BasicProxyInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: ProxyConfig,
) extends ProxyInstanceConfig

final case class ParticipantAwsKmsInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: ParticipantToAwsKms,
) extends ProxyInstanceConfig

final case class ParticipantGcpKmsInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: ParticipantToGcpKms,
) extends ProxyInstanceConfig

final case class ParticipantPostgresInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: ParticipantToPostgres,
    dbName: String,
    postgres: DbConfig.Postgres,
    dbTimeoutMillis: Long = 10000L,
) extends ProxyInstanceConfig

final case class MediatorPostgresInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: MediatorToPostgres,
    dbName: String,
    postgres: DbConfig.Postgres,
    dbTimeoutMillis: Long = 10000L,
) extends ProxyInstanceConfig

final case class SequencerPostgresInstanceConfig(
    name: String,
    upstreamHost: String,
    upstreamPort: Port,
    from: SequencerToPostgres,
    dbName: String,
    postgres: DbConfig.Postgres,
    dbTimeoutMillis: Long = 10000L,
) extends ProxyInstanceConfig
