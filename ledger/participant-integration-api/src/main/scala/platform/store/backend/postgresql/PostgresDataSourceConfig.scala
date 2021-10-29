// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue

case class PostgresDataSourceConfig(
    synchronousCommit: Option[SynchronousCommitValue] = None,
    // TCP keepalive configuration for postgres. See https://www.postgresql.org/docs/13/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS for details
    tcpKeepalivesIdle: Option[Int] = None, // corresponds to: tcp_keepalives_idle
    tcpKeepalivesInterval: Option[Int] = None, // corresponds to: tcp_keepalives_interval
    tcpKeepalivesCount: Option[Int] = None, // corresponds to: tcp_keepalives_count
)

object PostgresDataSourceConfig {
  sealed abstract class SynchronousCommitValue(val pgSqlName: String)
  object SynchronousCommitValue {
    case object On extends SynchronousCommitValue("on")
    case object Off extends SynchronousCommitValue("off")
    case object RemoteWrite extends SynchronousCommitValue("remote_write")
    case object RemoteApply extends SynchronousCommitValue("remote_apply")
    case object Local extends SynchronousCommitValue("local")
  }
}
