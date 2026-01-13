// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  ComponentHealthState,
  HealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasRunOnClosing,
  OnShutdownRunner,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.MonadUtil
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.Materializer

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
  *     [[com.digitalasset.canton.sequencing.InternalSequencerConnectionX.SequencerConnectionXState.Validated]])
  *     and agree on bootstrap information (synchronizer ID, static parameters) before the pool is
  *     initialized and starts serving connections.
  *   - It is the threshold determining the pool's health. After initialization and during the life
  *     of the pool, connections will be removed as they fail and added as they recover. The pool is
  *     then considered healthy if the number of connections (to different logical sequencers) is >=
  *     the threshold, degraded if it is below, and failing if it reaches 0.
  *
  * The configuration can also optionally define an expected
  * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]. If defined, any connection that
  * does not report connecting to that synchronizer will be rejected. If undefined, the synchronizer
  * ID will be determined by consensus once trust-threshold-many connections (to different logical
  * sequencers) report the same synchronizer.
  */
trait SequencerConnectionXPool extends FlagCloseable with NamedLogging {
  import SequencerConnectionXPool.*

  /** Return the synchronizer ID to which the connections in the pool are connected. Empty if the
    * pool has not yet reached enough validated connections to initialize.
    */
  def physicalSynchronizerIdO: Option[PhysicalSynchronizerId]

  /** Return the static parameters of the synchronizer to which the connections in the pool are
    * connected. Empty if the pool has not yet reached enough validated connections to initialize.
    */
  def staticSynchronizerParametersO: Option[StaticSynchronizerParameters]

  /** Start the connection pool. This will start all the configured connections and begin validating
    * them.
    *
    * @return
    *   A future that completes either when the connection pool has initialized and is serving
    *   connections, or when the initialization has timed out. The value of the timeout is defined
    *   by the [[com.digitalasset.canton.config.ProcessingTimeout.sequencerInfo]] configuration. In
    *   case of timeout, the pool is closed and unusable. A new connection pool must be created if
    *   desired.
    */
  def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXPoolError, Unit]

  /** Return the current configuration of the pool.
    */
  def config: SequencerConnectionXPoolConfig

  /** Dynamically update the pool configuration. Changing the expected synchronizer ID is not
    * supported and requires a node restart.
    */
  def updateConfig(newConfig: SequencerConnectionXPoolConfig)(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXPoolError, Unit]

  def health: SequencerConnectionXPoolHealth

  /** Return the health status of the connections. */
  def getConnectionsHealthStatus: Seq[HealthQuasiComponent]

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
      requester: String,
      nb: PositiveInt,
      exclusions: Set[SequencerId],
  )(implicit traceContext: TraceContext): Set[SequencerConnectionX]

  /** Obtain a single connection for each different sequencer ID present in the pool.
    */
  def getOneConnectionPerSequencer(requester: String)(implicit
      traceContext: TraceContext
  ): Map[SequencerId, SequencerConnectionX]

  /** Obtain all the connections present in the pool. */
  def getAllConnections()(implicit traceContext: TraceContext): Seq[SequencerConnectionX]

  /** Determine whether the connection pool can still reach the given threshold, ignoring the
    * `ignored` connections and considering an additional `extraUndecided` number of undecided
    * connections.
    */
  def isThresholdStillReachable(
      threshold: PositiveInt,
      ignored: Set[ConnectionXConfig] = Set.empty,
      extraUndecided: NonNegativeInt = NonNegativeInt.zero,
  )(implicit traceContext: TraceContext): Boolean

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
    * @param minRestartConnectionDelay
    *   The minimum duration after which a failed connection is restarted.
    * @param maxRestartConnectionDelay
    *   The maximum duration after which a failed connection is restarted.
    * @param warnConnectionValidationDelay
    *   The duration after which a warning is issued if a started connection still fails validation.
    * @param expectedPSIdO
    *   If provided, defines the synchronizer to which the connections are expected to connect. If
    *   empty, the synchronizer will be determined as soon as [[trustThreshold]]-many connections
    *   are validated and agree on bootstrap information.
    */
  final case class SequencerConnectionXPoolConfig(
      connections: NonEmpty[Seq[ConnectionXConfig]],
      trustThreshold: PositiveInt,
      minRestartConnectionDelay: NonNegativeFiniteDuration,
      maxRestartConnectionDelay: NonNegativeFiniteDuration,
      warnConnectionValidationDelay: NonNegativeFiniteDuration,
      expectedPSIdO: Option[PhysicalSynchronizerId] = None,
  ) extends PrettyPrinting {
    // TODO(i24780): when persisting, use com.digitalasset.canton.version.Invariant machinery for validation
    import SequencerConnectionXPoolConfig.*

    override protected def pretty: Pretty[SequencerConnectionXPoolConfig] = prettyOfClass(
      param("connections", _.connections),
      param("trustThreshold", _.trustThreshold),
      param("minRestartConnectionDelay", _.minRestartConnectionDelay),
      param("maxRestartConnectionDelay", _.maxRestartConnectionDelay),
      param("warnConnectionValidationDelay", _.warnConnectionValidationDelay),
      paramIfDefined("expectedPSIdO", _.expectedPSIdO),
    )

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
        _ <- Either.cond(
          minRestartConnectionDelay <= maxRestartConnectionDelay,
          (),
          s"Minimum restart connection delay ($minRestartConnectionDelay) must not exceed the maximum ($maxRestartConnectionDelay)",
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
    private[sequencing] final case class ChangedConnections(
        added: Set[ConnectionXConfig],
        removed: Set[ConnectionXConfig],
    ) extends PrettyPrinting {
      override protected def pretty: Pretty[ChangedConnections] = prettyOfClass(
        param("added", _.added),
        param("removed", _.removed),
      )
    }

    /** Create a sequencer connection pool configuration from the existing format.
      *
      * TODO(i27260): remove when no longer needed
      */
    def fromSequencerConnections(
        sequencerConnections: SequencerConnections,
        tracingConfig: TracingConfig,
        expectedPSIdO: Option[PhysicalSynchronizerId],
    ): SequencerConnectionXPoolConfig = {
      val connectionsConfig = sequencerConnections.aliasToConnection.flatMap {
        case (
              _,
              GrpcSequencerConnection(
                endpoints,
                transportSecurity,
                customTrustCertificates,
                sequencerAlias,
                expectedSequencerIdO,
              ),
            ) =>
          // In the current format, sequencers can have several connections if they are HA.
          // To get unique connection names, we name the connections using the sequencer alias followed by an
          // index corresponding to the connection.
          // In other words, for a given sequencer, the connections will be named "<sequencer alias>-0",
          // "<sequencer alias>-1", etc.
          val baseName = sequencerAlias.unwrap
          endpoints.zipWithIndex.map { case (ep, index) =>
            ConnectionXConfig(
              name = s"$baseName-$index",
              endpoint = ep,
              transportSecurity = transportSecurity,
              customTrustCertificates = customTrustCertificates,
              expectedSequencerIdO = expectedSequencerIdO,
              tracePropagation = tracingConfig.propagation,
            )
          }
      }.toSeq

      new SequencerConnectionXPoolConfig(
        connectionsConfig,
        trustThreshold = sequencerConnections.sequencerTrustThreshold,
        expectedPSIdO = expectedPSIdO,
        minRestartConnectionDelay =
          sequencerConnections.sequencerConnectionPoolDelays.minRestartDelay,
        maxRestartConnectionDelay =
          sequencerConnections.sequencerConnectionPoolDelays.maxRestartDelay,
        warnConnectionValidationDelay =
          sequencerConnections.sequencerConnectionPoolDelays.warnValidationDelay,
      )
    }

  }

  class SequencerConnectionXPoolHealth(
      override val name: String,
      override protected val associatedHasRunOnClosing: HasRunOnClosing,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected val initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
  }

  object SequencerConnectionXPoolHealth {
    class AlwaysHealthy(
        override val name: String,
        protected override val logger: TracedLogger,
    ) extends SequencerConnectionXPoolHealth(
          name,
          new OnShutdownRunner.PureOnShutdownRunner(logger),
          logger,
        ) {
      override protected val initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
      override val closingState: ComponentHealthState = ComponentHealthState.Ok()
    }
  }

  sealed trait SequencerConnectionXPoolError

  object SequencerConnectionXPoolError {
    final case class InvalidConfigurationError(error: String) extends SequencerConnectionXPoolError
    final case class TimeoutError(error: String) extends SequencerConnectionXPoolError
    final case class ThresholdUnreachableError(error: String) extends SequencerConnectionXPoolError
  }
}

trait SequencerConnectionXPoolFactory {
  import SequencerConnectionXPool.{SequencerConnectionXPoolConfig, SequencerConnectionXPoolError}

  def create(
      initialConfig: SequencerConnectionXPoolConfig,
      name: String,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool]

  // TODO(i27260): remove when no longer needed
  def createFromOldConfig(
      sequencerConnections: SequencerConnections,
      expectedPSIdO: Option[PhysicalSynchronizerId],
      tracingConfig: TracingConfig,
      name: String,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool]
}
