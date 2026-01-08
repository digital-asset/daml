// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration of node replication for high availability
  *
  * @param enabled
  *   Needs to be set to true for a replicated node, which shares the database with other replicas.
  * @param connectionPool
  *   Configuration for the write connection pool.
  */
final case class ReplicationConfig(
    // Optional to be able to know if the value has been set in the config file. This is useful to apply
    // default values later on, based on the type of storage used.
    enabled: Option[Boolean] = None,
    connectionPool: DbLockedConnectionPoolConfig = DbLockedConnectionPoolConfig(),
) {
  lazy val isEnabled: Boolean = enabled.contains(true)
}

object ReplicationConfig {

  def withDefault(
      storage: StorageConfig,
      enabled: Option[Boolean],
  ): Option[Boolean] =
    // If replication has not been set explicitly in the conf file and storage supports it, enable it by default
    enabled.orElse(Option.when(DbLockConfig.isSupportedConfig(storage))(true))

  def withDefaultO(
      storage: StorageConfig,
      replicationO: Option[ReplicationConfig],
  ): Option[ReplicationConfig] = {
    val enabled = withDefault(storage, replicationO.flatMap(_.enabled))
    replicationO
      .map(_.copy(enabled = enabled))
      .orElse(enabled.map(enabled => ReplicationConfig(enabled = Some(enabled))))
  }

  val disabled: ReplicationConfig = ReplicationConfig(enabled = Some(false))
}
