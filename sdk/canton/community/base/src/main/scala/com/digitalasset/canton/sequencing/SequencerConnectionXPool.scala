// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import scala.concurrent.ExecutionContextExecutor

/** Pool of sequencer connections.
  *
  * The pool is initialized with a configuration describing the individual connections information
  * (endpoint, TLS, etc.) and a trust threshold.
  *
  * The connections information can combine connections to different logical sequencers (sequencer
  * ID) as well as multiple endpoints for the same logical sequencer, which allows for
  * high-availability for a given sequencer. The contents of the pool then distinguishes between the
  * number of logical sequencers represented [[nbSequencers]] and the number of individual
  * connections [[nbConnections]].
  *
  * The trust threshold has the following functions:
  *   - It represents the number of connections (to different logical sequencers) that must be
  *     validated (see
  *     [[com.digitalasset.canton.sequencing.SequencerConnectionX.SequencerConnectionXState.Validated]])
  *     and agree on bootstrap information (synchronizer ID, static parameters) before the pool is
  *     initialized and starts serving connections.
  *   - It is the threshold determining the pool's health. After initialization and during the life
  *     of the pool, connections will be removed as they fail and added as they recover. The pool is
  *     then considered healthy if the number of connections (to different logical sequencers) is >=
  *     the threshold, degraded if it is below, and failing if it reaches 0.
  *
  * The configuration can also optionally define an expected
  * [[com.digitalasset.canton.topology.SynchronizerId]]. If defined, any connection that does not
  * report connecting to that synchronizer will be rejected. If undefined, the synchronizer ID will
  * be determined by consensus once trust-threshold-many connections (to different logical
  * sequencers) report the same synchronizer.
  */
trait SequencerConnectionXPool extends FlagCloseable with NamedLogging {
  import SequencerConnectionXPool.*

  /** Return the synchronizer ID to which the connections in the pool are connected. Empty if the
    * pool has not yet reached enough validated connections to initialize.
    */
  def synchronizerId: Option[SynchronizerId]

  def start()(implicit traceContext: TraceContext): Unit

  /** Return the current configuration of the pool.
    */
  def config: SequencerConnectionXPoolConfig

  /** Dynamically update the pool configuration. Changing the expected synchronizer ID is not
    * supported and requires a node restart.
    */
  def updateConfig(newConfig: SequencerConnectionXPoolConfig)(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXPool.SequencerConnectionXPoolError, Unit]

  def health: SequencerConnectionXPoolHealth

  /** Return the number of different sequencerIds present in the pool (connections for the same
    * sequencerId count for 1).
    */
  def nbSequencers: NonNegativeInt

  /** Return the number of connections present in the pool (>= [[nbSequencers]]).
    */
  def nbConnections: NonNegativeInt

  /** Obtain a number of connections from the pool. The set of returned connections has the
    * following properties:
    *   - it contains at most one connection per sequencer ID
    *   - the sequencer IDs represented are picked at random at every call
    *   - it may contain less than the requested number of connections if the current pool contents
    *     cannot satisfy the requirements
    *   - it excludes connections for sequencer IDs provided in `exclusions`
    *   - when the pool contains multiple connections for the same sequencer ID, the connection
    *     returned for that sequencer ID is chosen via round-robin
    */
  def getConnections(
      nb: Int,
      exclusions: Set[SequencerId],
  )(implicit traceContext: TraceContext): Set[SequencerConnectionX]

  @VisibleForTesting
  def contents: Map[SequencerId, Set[SequencerConnectionX]]
}

object SequencerConnectionXPool {

  /** Sequencer pool configuration
    *
    * @param connections
    *   Configurations for the individual connections.
    * @param trustThreshold
    *   Number of connections that need to be validated and agree on bootstrap information in order
    *   for the pool to initialize and start serving connections. After initialization, if the
    *   number of connections in the pool goes below the threshold, the pool's health will
    *   transition to `degraded` (or `failed` if it reaches 0).
    * @param restartConnectionDelay
    *   The duration after which a failed connection is restarted.
    * @param expectedSynchronizerIdO
    *   If provided, defines the synchronizer to which the connections are expected to connect. If
    *   empty, the synchronizer will be determined as soon as [[trustThreshold]]-many connections
    *   are validated and agree on bootstrap information.
    */
  final case class SequencerConnectionXPoolConfig(
      connections: NonEmpty[Seq[ConnectionXConfig]],
      trustThreshold: PositiveInt,
      restartConnectionDelay: Duration = Duration.ofMillis(500),
      expectedSynchronizerIdO: Option[SynchronizerId] = None,
  ) {
    // TODO(i24780): when persisting, use com.digitalasset.canton.version.Invariant machinery for validation
    import SequencerConnectionXPoolConfig.*

    def validate: Either[SequencerConnectionXPoolError, Unit] = {
      val (names, endpoints) = connections.map(conn => conn.name -> conn.endpoint).unzip

      val check = for {
        _ <- MonadUtil.foldLeftM(Set.empty[String], names)((seen, name) =>
          Either.cond(
            !seen.contains(name),
            seen + name,
            s"""Connection name "$name" is used for more than one connection""",
          )
        )
        _ <- MonadUtil.foldLeftM(Set.empty[Endpoint], endpoints)((seen, endpoint) =>
          Either.cond(
            !seen.contains(endpoint),
            seen + endpoint,
            s"""Connection endpoint "$endpoint" is used for more than one connection""",
          )
        )
        _ <- Either.cond(
          trustThreshold.unwrap <= connections.size,
          (),
          s"Trust threshold ($trustThreshold) must not exceed the number of connections (${connections.size})",
        )
      } yield ()

      check.leftMap(SequencerConnectionXPoolError.InvalidConfigurationError.apply)
    }

    def changedConnections(
        newConfig: SequencerConnectionXPoolConfig
    ): ChangedConnections = {
      val previousConnectionConfigs = this.connections.forgetNE.toSet
      val newConnectionConfigs = newConfig.connections.forgetNE.toSet

      val added = newConnectionConfigs.diff(previousConnectionConfigs)
      val removed = previousConnectionConfigs.diff(newConnectionConfigs)

      ChangedConnections(added, removed)
    }
  }

  object SequencerConnectionXPoolConfig {
    final case class ChangedConnections(
        added: Set[ConnectionXConfig],
        removed: Set[ConnectionXConfig],
    )
  }

  class SequencerConnectionXPoolHealth(
      override val name: String,
      override protected val associatedHasRunOnClosing: HasRunOnClosing,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected val initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
  }

  sealed trait SequencerConnectionXPoolError

  object SequencerConnectionXPoolError {
    final case class InvalidConfigurationError(error: String) extends SequencerConnectionXPoolError
  }
}

object SequencerConnectionXPoolFactory {
  import SequencerConnectionXPool.{SequencerConnectionXPoolConfig, SequencerConnectionXPoolError}

  def create(
      initialConfig: SequencerConnectionXPoolConfig,
      connectionFactory: SequencerConnectionXFactory,
      clock: Clock,
      seedForRandomnessO: Option[Long],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor
  ): Either[SequencerConnectionXPoolError, SequencerConnectionXPoolImpl] =
    for {
      _ <- initialConfig.validate
    } yield {
      new SequencerConnectionXPoolImpl(
        initialConfig,
        connectionFactory,
        clock,
        seedForRandomnessO,
        timeouts,
        loggerFactory,
      )
    }
}
