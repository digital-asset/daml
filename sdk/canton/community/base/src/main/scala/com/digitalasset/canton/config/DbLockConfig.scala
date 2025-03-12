// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

/** Configuration of a DB lock
  *
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between one health check completed and the next to
  *   start.
  * @param healthCheckTimeout
  *   Timeout for running a health check in seconds granularity.
  */
final case class DbLockConfig(
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    healthCheckTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
) extends EnterpriseOnlyCantonConfigValidation

object DbLockConfig {
  implicit val dbLockConfigCanontConfigValidator: CantonConfigValidator[DbLockConfig] =
    CantonConfigValidatorDerivation[DbLockConfig]

  /** For locks to be supported we must be using an [[DbConfig]] with it set to Postgres. */
  private[canton] def isSupportedConfig(config: StorageConfig): Boolean =
    PartialFunction.cond(config) { case _: DbConfig.Postgres =>
      true
    }

  // we need to preallocate a range of lock counters for concurrently running sequencer writers
  // this actually sets the limit of the number of concurrent sequencers that we allow
  val MAX_SEQUENCER_WRITERS_AVAILABLE = 32

}

/** Configuration of a DB-locked connection, i.e., a database connection with an associated DB lock.
  *
  * @param passiveCheckPeriod
  *   How long to wait between trying to become active.
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between one health check completed and the next to
  *   start.
  * @param healthCheckTimeout
  *   Timeout for running a health check in seconds granularity.
  * @param connectionTimeout
  *   Timeout for requesting a new connection from the underlying database driver in seconds
  *   granularity.
  * @param keepAliveIdle
  *   TCP keep-alive idle time, i.e., how long to wait until sending a keep-alive message when idle.
  * @param keepAliveInterval
  *   TCP keep-alive interval, i.e., how long to wait until resending an unanswered keep-alive
  *   message.
  * @param keepAliveCount
  *   TCP keep-alive count, i.e., how many unanswered keep-alive messages required to consider the
  *   connection lost.
  * @param initialAcquisitionMaxRetries
  *   Maximum number of retries when trying to acquire the lock for the first time before trying to
  *   acquire the lock in a background task.
  * @param initialAcquisitionInterval
  *   Retry intervals during the initial attempts to acquire the lock.
  * @param lock
  *   Configuration of the DB locks used by the pool.
  */
final case class DbLockedConnectionConfig(
    passiveCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    healthCheckTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
    connectionTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
    keepAliveIdle: Option[PositiveFiniteDuration] = None,
    keepAliveInterval: Option[PositiveFiniteDuration] = None,
    keepAliveCount: Option[Int] = None,
    initialAcquisitionMaxRetries: Int = 5,
    initialAcquisitionInterval: PositiveFiniteDuration = PositiveFiniteDuration.ofMillis(200),
    lock: DbLockConfig = DbLockConfig(),
) extends EnterpriseOnlyCantonConfigValidation

object DbLockedConnectionConfig {
  implicit val dbLockedConnectionConfigCantonConfigValidator
      : CantonConfigValidator[DbLockedConnectionConfig] =
    CantonConfigValidatorDerivation[DbLockedConnectionConfig]
}

/** Configuration for the connection pool using DB locks.
  *
  * @param healthCheckPeriod
  *   Health check period, i.e., how long to wait between health checks of the connection pool.
  * @param connection
  *   Configuration of the DB locked connection used by the pool.
  * @param activeTimeout
  *   Time to wait until the first connection in the pool becomes active during failover.
  */
final case class DbLockedConnectionPoolConfig(
    healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(5),
    connection: DbLockedConnectionConfig = DbLockedConnectionConfig(),
    activeTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(15),
) extends EnterpriseOnlyCantonConfigValidation

object DbLockedConnectionPoolConfig {
  implicit val dbLockedConnectionPoolConfigCantonConfigValidator
      : CantonConfigValidator[DbLockedConnectionPoolConfig] =
    CantonConfigValidatorDerivation[DbLockedConnectionPoolConfig]
}
