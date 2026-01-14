// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{HealthListener, HealthQuasiComponent}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXState,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.{
  SequencerConnectionXPoolConfig,
  SequencerConnectionXPoolError,
  SequencerConnectionXPoolHealth,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.collection.SeqUtil
import com.digitalasset.canton.util.{
  ErrorUtil,
  FutureUnlessShutdownUtil,
  LoggerUtil,
  Mutex,
  SingleUseCell,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.{immutable, mutable}
import scala.compat.java8.DurationConverters.DurationOps
import scala.concurrent.ExecutionContextExecutor
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

class SequencerConnectionXPoolImpl private[sequencing] (
    private val initialConfig: SequencerConnectionXPoolConfig,
    connectionFactory: InternalSequencerConnectionXFactory,
    clock: Clock,
    authConfig: AuthenticationTokenManagerConfig,
    member: Member,
    crypto: Crypto,
    seedForRandomnessO: Option[Long],
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor, esf: ExecutionSequencerFactory, materializer: Materializer)
    extends SequencerConnectionXPool {

  import SequencerConnectionXPoolImpl.*

  /** Use a wall clock for scheduling restart delays, so that the pool can make progress even in
    * tests that use static time without explicitly advancing the time
    */
  private val wallClock = new WallClock(timeouts, loggerFactory)

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[SequencerConnectionXPoolConfig](initialConfig)

  /** Flag indicating whether the connection pool has been started */
  private val startedRef = new AtomicBoolean(false)

  /** The pool itself, as a sequence of connections per sequencer ID */
  private val pool = mutable.Map[SequencerId, NonEmpty[Seq[SequencerConnectionX]]]()

  /** Tracks the configured connections, along with their validated status */
  private val trackedConnections: mutable.Map[InternalSequencerConnectionX, Boolean] =
    mutable.HashMap[InternalSequencerConnectionX, Boolean]()

  /** Bootstrap information once the pool has been initialized */
  private val bootstrapCell = new SingleUseCell[BootstrapInfo]

  /** Used to provide randomness in connection selection */
  private val random = seedForRandomnessO.fold(new Random)(new Random(_))

  /** Used to synchronize access to the mutable structures [[pool]] and [[trackedConnections]]
    */
  private val lock = new Mutex()

  /** Promise that is satisfied when the pool completes initialization, or the
    * [[com.digitalasset.canton.config.ProcessingTimeout.sequencerInfo]] timeout value has elapsed
    */
  private val initializedP = {
    import TraceContext.Implicits.Empty.*
    PromiseUnlessShutdown
      .supervised[Either[SequencerConnectionXPoolError, Unit]](
        "connection-pool-initialization",
        futureSupervisor,
      )
  }

  override def config: SequencerConnectionXPoolConfig = configRef.get

  override val health: SequencerConnectionXPoolHealth = new SequencerConnectionXPoolHealth(
    name = "sequencer-connection-pool",
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  override def getConnectionsHealthStatus: Seq[HealthQuasiComponent] =
    lock.exclusive {
      trackedConnections.toSeq.map { case (connection, _) => connection.health }
    }

  @VisibleForTesting
  override def contents: Map[SequencerId, Set[SequencerConnectionX]] =
    lock.exclusive {
      pool.view.mapValues(_.toSet).toMap
    }

  override def physicalSynchronizerIdO: Option[PhysicalSynchronizerId] =
    bootstrapCell.get.map(_.synchronizerId)

  override def staticSynchronizerParametersO: Option[StaticSynchronizerParameters] =
    bootstrapCell.get.map(_.staticParameters)

  private implicit def mc: MetricsContext = metricsContext
  metrics.trustThreshold.updateValue(config.trustThreshold.value)

  override def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXPoolError, Unit] = {
    if (startedRef.getAndSet(true)) {
      logger.debug("Connection pool already started -- ignoring")
    } else {
      logger.debug(s"Starting connection pool")
      setupInitializationTimeout()

      updateTrackedConnections(toBeAdded = config.connections, toBeRemoved = Set.empty)
    }

    EitherT(initializedP.futureUS)
  }

  private def setupInitializationTimeout()(implicit traceContext: TraceContext): Unit = {
    val initializationTimeout = timeouts.sequencerInfo

    def signalTimeout(): Unit = {
      val timeoutMessage = s"Connection pool failed to initialize within " +
        s"${LoggerUtil.roundDurationForHumans(initializationTimeout.duration)}"
      if (initializedP.outcome(Left(SequencerConnectionXPoolError.TimeoutError(timeoutMessage)))) {
        logger.info(s"$timeoutMessage -- closing the pool")
        close()
      }
    }

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      wallClock.scheduleAfter(_ => signalTimeout(), initializationTimeout.asJavaApproximation),
      s"connection-pool-initialization-timeout",
    )
  }

  override def isThresholdStillReachable(
      threshold: PositiveInt,
      ignored: Set[ConnectionXConfig],
      extraUndecided: NonNegativeInt,
  )(implicit traceContext: TraceContext): Boolean =
    lock.exclusive {
      // Grab all attributes only once to prevent a race condition if a connection is validated between the next calls
      val nonFatalAttributes = trackedConnections.toSeq.collect {
        case (connection, _validated)
            if !connection.health.getState.isFatal && !ignored.contains(connection.config) =>
          connection.attributes
      }

      // Number of unique sequencer IDs that have been validated at some point, differentiated by bootstrap
      val validatedSequencerIds = nonFatalAttributes.flatten.toSet
        .groupMap(BootstrapInfo.fromAttributes)(_.sequencerId)
        .values
        .map(_.size)
      // Maximum of these
      val maxValidatedSequencerIds = validatedSequencerIds.maxOption.getOrElse(0)
      // Number of connection that have not yet been validated (and therefore have not yet determined their sequencer ID)
      val undecided = nonFatalAttributes.count(_.isEmpty)

      val isReachable =
        maxValidatedSequencerIds + undecided + extraUndecided.unwrap >= threshold.unwrap

      logger.debug(
        s"isThresholdStillReachable = $isReachable: threshold = $threshold; ignored = ${ignored.size} -> "
          + s"validatedSequencerIds = $validatedSequencerIds; undecided = $undecided + $extraUndecided"
      )

      isReachable
    }

  private def updateTrackedConnections(
      toBeAdded: immutable.Iterable[ConnectionXConfig],
      toBeRemoved: Set[ConnectionXConfig],
  )(implicit traceContext: TraceContext): Unit =
    lock.exclusive {
      val removedConnections =
        trackedConnections.keySet.filter(connection => toBeRemoved.contains(connection.config))
      val newConnections = toBeAdded.map(connectionFactory.create)

      trackedConnections.filterInPlace { case (connection, _) =>
        !removedConnections.contains(connection)
      }
      removedConnections.foreach { connection =>
        removeConnectionFromPool(connection)
      }
      // Newly created connections are not yet validated
      trackedConnections.addAll(newConnections.map(_ -> false))

      metrics.trackedConnections.updateValue(trackedConnections.size)

      // Note that the following calls to `connection.fatal` and `startConnection` can potentially trigger
      // further processing via health state changes and modify the tracked connections or the pool.
      // Even if this happens in the same thread, it does not interfere with our processing.
      removedConnections.foreach { connection =>
        connection.fatal("removed from config")
      }
      // If start() or updateConfig() is called after the pool has been closed, we don't want to start new connections
      if (!isClosing) {
        newConnections.map(new ConnectionHandler(_)).foreach(_.startConnection())
      }
    }

  private class ConnectionHandler(connection: InternalSequencerConnectionX) extends NamedLogging {
    protected override val loggerFactory: NamedLoggerFactory =
      SequencerConnectionXPoolImpl.this.loggerFactory
        .append("connection", s"${connection.config.name}")

    /** @param scheduled
      *   Indicates that a restart has already been scheduled for this connection
      * @param delay
      *   Current restart delay. It grows exponentially at every restart, and is reset to the
      *   minimum when a connection fails after having been validated.
      */
    private case class RestartData(
        scheduled: Boolean,
        delay: NonNegativeFiniteDuration,
        initialStartTimeO: Option[CantonTimestamp],
    )

    private val restartDataRef = new AtomicReference[RestartData](
      RestartData(
        scheduled = false,
        delay = config.minRestartConnectionDelay,
        initialStartTimeO = None,
      )
    )

    private def resetRestartDelay(): Unit = restartDataRef.getAndUpdate {
      _.copy(
        delay = config.minRestartConnectionDelay,
        initialStartTimeO = None,
      )
    }.discard

    private def scheduleRestart()(implicit traceContext: TraceContext): Unit = {
      val RestartData(restartAlreadyScheduled, delay, initialStartTimeO) =
        restartDataRef.getAndUpdate {
          case current @ RestartData(false, delay, _) =>
            current.copy(
              scheduled = true,
              // Exponentially backoff the restart delay, bounded by the max
              delay = (delay * NonNegativeInt.two).min(config.maxRestartConnectionDelay),
            )
          case other => other
        }

      initialStartTimeO.foreach { initialStartTime =>
        val durationSinceInitialStart = (wallClock.now - initialStartTime).toScala
        if (durationSinceInitialStart > config.warnConnectionValidationDelay.toScala) {
          logger.warn(
            s"Connection has failed validation since $initialStartTime" +
              s" (${LoggerUtil.roundDurationForHumans(durationSinceInitialStart)} ago)." +
              s" Last failure reason: \"${connection.lastFailureReason.getOrElse("N/A")}\""
          )
        }
      }

      if (restartAlreadyScheduled)
        logger.debug("Restart already scheduled -- ignoring")
      else {
        logger.info(
          s"Scheduling restart after ${LoggerUtil.roundDurationForHumans(delay.toScala)}"
        )
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          wallClock.scheduleAfter(_ => restart(), delay.duration),
          s"restart-connection-${connection.name}",
        )
      }
    }

    private def restart()(implicit traceContext: TraceContext): Unit =
      if (!isClosing) {
        if (restartDataRef.getAndUpdate(_.copy(scheduled = false)).scheduled) {
          logger.debug("Restarting")
          startConnection()
        }
      }

    def startConnection()(implicit traceContext: TraceContext): Unit = {
      restartDataRef.getAndUpdate {
        case current @ RestartData(_, _, None) =>
          current.copy(initialStartTimeO = Some(wallClock.now))
        case other => other
      }.discard

      connection
        .start()
        .valueOr(err => logger.warn(s"Failed to start connection ${connection.name}: $err"))
    }

    private def register(): Unit =
      connection.health
        // We register on high priority to ensure that the pool is notified first of closing connections,
        // before propagating the state to other clients.
        // Indeed, clients of the pool are likely to register on a connection to be notified when it has stopped.
        // As a consequence of the notification, they will request a new connection. If they are notified before the
        // connection pool itself, they may receive the same connection again since the pool has not yet removed it
        // from its contents. Then they will register, be notified that it is bad, request a new one, etc.
        .registerHighPriorityOnHealthChange(new HealthListener {
          override def name: String = s"SequencerConnectionPool-${connection.name}"

          override def poke()(implicit traceContext: TraceContext): Unit = {
            val state = connection.health.getState
            logger.debug(s"Poked with state $state")

            state match {
              case SequencerConnectionXState.Validated => processValidatedConnection(connection)

              case SequencerConnectionXState.Stopped(_) =>
                removeConnectionFromPool(connection, actionIfPresent = resetRestartDelay())
                if (!isClosing) scheduleRestart()

              // For any other state, ensure the connection is not in the pool
              case SequencerConnectionXState.Fatal(_) | SequencerConnectionXState.Initial |
                  SequencerConnectionXState.Starting | SequencerConnectionXState.Started |
                  SequencerConnectionXState.Stopping(_) =>
                if (state.isFatal)
                  checkIfThresholdIsStillReachable(config.trustThreshold)
                removeConnectionFromPool(connection, actionIfPresent = resetRestartDelay())
            }
          }
        })
        .discard[Boolean]

    register()
  }

  override def updateConfig(
      newConfig: SequencerConnectionXPoolConfig
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXPoolError, Unit] =
    lock.exclusive {
      val currentConfig = config
      val newThreshold = newConfig.trustThreshold
      val poolNotInitialized = bootstrapCell.get.isEmpty

      for {
        _ <- newConfig.validate
        _ <- Either.cond(
          newConfig.expectedPSIdO == currentConfig.expectedPSIdO,
          (),
          SequencerConnectionXPoolError.InvalidConfigurationError(
            "The expected physical synchronizer ID can only be changed during a node restart."
          ),
        )

        changedConnections = currentConfig.changedConnections(newConfig)

        // Check whether the trust threshold is reachable with the new configuration
        _ <-
          Either.cond(
            isThresholdStillReachable(
              newThreshold,
              // The removed connections don't count
              ignored = changedConnections.removed,
              // The added connections are so far undecided
              extraUndecided = NonNegativeInt.tryCreate(changedConnections.added.size),
            ),
            (),
            SequencerConnectionXPoolError.InvalidConfigurationError(
              s"Trust threshold $newThreshold cannot be reached"
            ),
          )

        // Check whether the new threshold is now reached, which can only happen if it was lowered
        bootstrapIfThresholdReachedO <-
          if (poolNotInitialized && newThreshold < currentConfig.trustThreshold) {
            getBootstrapIfThresholdReached(
              newThreshold,
              // Removed connections must not be counted.
              // We don't remove them yet from the tracked connections in case many bootstraps reach the
              // threshold and the config gets rejected.
              ignored = changedConnections.removed,
            )
              .leftMap(SequencerConnectionXPoolError.InvalidConfigurationError.apply)
          } else {
            Right(None)
          }
      } yield {
        configRef.set(newConfig)
        logger.info(s"Configuration updated to: $newConfig")

        metrics.trustThreshold.updateValue(config.trustThreshold.value)

        // If the trust threshold is now reached, process it
        bootstrapIfThresholdReachedO.foreach(initializePool)

        logger.debug(
          s"Configuration update triggers the following connection changes: $changedConnections"
        )
        updateTrackedConnections(
          toBeAdded = changedConnections.added,
          toBeRemoved = changedConnections.removed,
        )
      }
    }

  private def markValidated(
      connection: InternalSequencerConnectionX
  )(implicit traceContext: TraceContext): Unit = {
    {
      lock.exclusive {
        require(trackedConnections.contains(connection))
        trackedConnections.update(connection, true)
      }
    }
    checkIfThresholdIsStillReachable(config.trustThreshold)
  }

  private def checkIfThresholdIsStillReachable(
      threshold: PositiveInt
  )(implicit traceContext: TraceContext): Unit =
    if (!isClosing && !isThresholdStillReachable(threshold)) {
      val message =
        s"Trust threshold of ${config.trustThreshold} is no longer reachable"
      logger.info(message)
      if (
        initializedP
          .outcome(
            Left(SequencerConnectionXPoolError.ThresholdUnreachableError(message))
          )
      ) {
        // Close if we have not yet initialized
        logger.info("Connection pool failed to initialize -- closing")
        close()
      }
    }

  /** Check whether a group of validated connections reaches the threshold
    *
    * @param ignored
    *   Connection configs which should be ignored for checking the threshold
    * @return
    *   Either an optional bootstrap info if the threshold is reached, or an error if more than one
    *   bootstrap info has reached the threshold.
    */
  private def getBootstrapIfThresholdReached(
      threshold: PositiveInt,
      ignored: Set[ConnectionXConfig] = Set.empty,
  ): Either[String, Option[BootstrapInfo]] = {
    // No need to synchronize here as all callers have already acquired the lock

    val attributesOfValidatedConnections = trackedConnections.toSeq.mapFilter {
      case (connection, validated) if validated && !ignored.contains(connection.config) =>
        checked(Some(connection.tryAttributes))
      case _ => None
    }.toSet
    // We build this map for every check as it makes it easier to handle adding and removing connections.
    // If this becomes a concern, we can optimize it in the future.
    val bootstrapToSequencerIds = attributesOfValidatedConnections
      .groupMap(BootstrapInfo.fromAttributes)(_.sequencerId)

    bootstrapToSequencerIds.toSeq.mapFilter {
      case (bootstrapInfo, sequencerIds) if sequencerIds.sizeIs >= threshold.unwrap =>
        Some(bootstrapInfo)
      case _ => None
    } match {
      case Seq() => Right(None)
      case Seq(bootstrapInfo) => Right(Some(bootstrapInfo))
      case many =>
        Left(
          s"Trust threshold of $threshold is reached with multiple bootstrap infos: $many." +
            s" Please configure a higher trust threshold."
        )
    }
  }

  private def initializePool(
      bootstrapInfo: BootstrapInfo
  )(implicit traceContext: TraceContext): Unit =
    // No need to synchronize here as all callers have already acquired the lock
    bootstrapCell.putIfAbsent(bootstrapInfo) match {
      case None =>
        logger.debug(
          s"Trust threshold of ${config.trustThreshold} reached with $bootstrapInfo"
        )
        trackedConnections.foreach {
          case (connection, validated) if validated =>
            val attributes = checked(connection.tryAttributes)
            val bootstrapInfoFromConnection = BootstrapInfo.fromAttributes(attributes)

            if (bootstrapInfoFromConnection == bootstrapInfo) {
              addConnectionToPool(connection, attributes.sequencerId, attributes.staticParameters)
            } else {
              invalidBootstrap(connection, bootstrapInfo, bootstrapInfoFromConnection)
            }
          case _ =>
        }
        initializedP.outcome_(Right(()))

      case _ => ErrorUtil.invalidState("The pool has already been initialized")
    }

  override def nbSequencers: NonNegativeInt =
    lock.exclusive(NonNegativeInt.tryCreate(pool.size))

  override def nbConnections: NonNegativeInt =
    lock.exclusive(NonNegativeInt.tryCreate(pool.values.flatten.size))

  override def onClosed(): Unit = {
    val instances = (lock.exclusive(trackedConnections.keySet.toSeq))

    initializedP.shutdown_()
    // We close the connections outside the critical section to avoid shutdown problems in case
    // it triggers health callbacks
    LifeCycle.close(instances*)(logger)
    super.onClosed()
  }

  private def processValidatedConnection(connection: InternalSequencerConnectionX)(implicit
      traceContext: TraceContext
  ): Unit =
    lock.exclusive {
      if (isClosing) connection.fatal("closing due to shutdown")
      else {
        val currentConfig = config
        val attributes = checked(connection.tryAttributes)
        val bootstrapInfo = BootstrapInfo.fromAttributes(attributes)

        logger.debug(s"Processing validated connection ${connection.name} with $bootstrapInfo")

        bootstrapCell.get match {
          case None =>
            if (currentConfig.expectedPSIdO.forall(_ == attributes.physicalSynchronizerId)) {
              markValidated(connection)
              getBootstrapIfThresholdReached(currentConfig.trustThreshold)
                .valueOr(ErrorUtil.invalidState(_)) // We cannot reach the threshold multiple times
                .fold(
                  logger.debug(
                    s"Trust threshold of ${currentConfig.trustThreshold} not yet reached"
                  )
                )(initializePool)
            } else {
              logger.warn(
                s"Connection ${connection.name} is not on expected synchronizer:" +
                  s" expected ${currentConfig.expectedPSIdO}," +
                  s" got ${attributes.physicalSynchronizerId}. Closing connection."
              )
              connection.fatal(reason = "invalid synchronizer")
            }

          case Some(`bootstrapInfo`) =>
            addConnectionToPool(connection, attributes.sequencerId, attributes.staticParameters)

          case Some(currentBootstrapInfo) =>
            invalidBootstrap(connection, currentBootstrapInfo, bootstrapInfo)
        }
      }
    }

  private def invalidBootstrap(
      connection: InternalSequencerConnectionX,
      good: BootstrapInfo,
      bad: BootstrapInfo,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.warn(
      s"Connection ${connection.name} has invalid bootstrap info: expected $good, got $bad. Closing connection."
    )
    connection.fatal(reason = "invalid bootstrap info")
  }

  private def addConnectionToPool(
      connection: InternalSequencerConnectionX,
      sequencerId: SequencerId,
      staticParameters: StaticSynchronizerParameters,
  )(implicit traceContext: TraceContext): Unit =
    lock.exclusive {
      logger.debug(s"Adding ${connection.name} to the pool")

      val synchronizerCrypto = SynchronizerCrypto(crypto, staticParameters)
      connection
        .buildUserConnection(authConfig, member, synchronizerCrypto, clock)
        .map { userConnection =>
          pool
            .updateWith(sequencerId) {
              case None => Some(NonEmpty.mk(Seq, userConnection))
              // Match on config
              case Some(current) if current.exists(_.config == connection.config) =>
                // Can happen if a connection is poked several times with the 'validated' state
                logger.debug(
                  s"Connection ${connection.name} is already present in the pool for $sequencerId"
                )
                // This user connection instance is discarded
                userConnection.close()

                Some(current)
              case Some(current) =>
                Some(current :+ userConnection)
            }
            .discard

          metrics.validatedConnections.updateValue(pool.size)
          updateHealth()
        }
        .valueOr { error =>
          // Can happen if the connection was closed concurrently (channel not available).
          // The connection will be restarted if it did not close for a fatal reason.
          logger.debug(s"Failed to attach authentication to connection ${connection.name}: $error")
        }
    }

  /** @param actionIfPresent
    *   An action to perform if the connection is effectively present in the pool (which is not
    *   always the case due to concurrent activity)
    */
  private def removeConnectionFromPool(
      connection: InternalSequencerConnectionX,
      actionIfPresent: => Unit = (),
  )(implicit traceContext: TraceContext): Unit =
    lock.exclusive {
      connection.attributes match {
        case Some(attributes) =>
          val sequencerId = attributes.sequencerId

          pool
            .updateWith(sequencerId) {
              // Match on config
              case Some(current) if current.exists(_.config == connection.config) =>
                logger.debug(s"Removing ${connection.name} from the pool")
                actionIfPresent
                val newList = current.filter(_.config != connection.config)
                NonEmpty.from(newList)
              case None => None
              case Some(current) => Some(current)
            }
            .discard

          metrics.validatedConnections.updateValue(pool.size)
          updateHealth()

        case None =>
        // The connection has never been validated, and therefore cannot be in the pool
      }
    }

  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val trustThreshold = config.trustThreshold
    nbSequencers match {
      case nb if nb >= trustThreshold => health.resolveUnhealthy()
      case nb if nb > NonNegativeInt.zero =>
        health.degradationOccurred(
          s"only $nb connection(s) to different sequencers available, trust threshold = $trustThreshold"
        )
      case _ => health.failureOccurred("no connection available")
    }
  }

  override def getConnections(
      requester: String,
      requestedNumber: PositiveInt,
      exclusions: Set[SequencerId],
  )(implicit traceContext: TraceContext): Set[SequencerConnectionX] =
    // Always return connections randomly for now
    lock.exclusive {
      logger.debug(
        s"[$requester] requesting $requestedNumber connection(s) excluding ${exclusions.map(_.uid.identifier)}"
      )

      // Pick up to `requestedNumber` non-excluded sequencer IDs from the pool
      val randomSeqIds = SeqUtil.randomSubsetShuffle(
        pool.keySet.diff(exclusions).toIndexedSeq,
        requestedNumber.unwrap,
        random,
      )

      val pickedConnections = randomSeqIds.map { seqId =>
        val connections = pool(seqId) // seqIDs were picked from the pool so this cannot fail

        // Cheap round-robin: return the head connection and move it to the end
        val head = connections.head1
        val newList = NonEmpty.from(connections.tail1) match {
          case None => NonEmpty.mk(Seq, head)
          case Some(nonEmptyTail) => nonEmptyTail :+ head
        }
        pool.update(seqId, newList)

        head
      }.toSet

      logger.debug(s"[$requester] returning ${pickedConnections.map(_.name)}")
      pickedConnections
    }

  override def getOneConnectionPerSequencer(requester: String)(implicit
      traceContext: TraceContext
  ): Map[SequencerId, SequencerConnectionX] = {
    logger.debug(s"[$requester] requesting one connection per sequencer")
    // Upper bound on number of connections. Note: `checked` because `connections` is `NonEmpty`.
    val nb = checked(PositiveInt.tryCreate(config.connections.size))
    getConnections(requester, nb, exclusions = Set.empty)
      .map(c => c.attributes.sequencerId -> c)
      .toMap
  }

  override def getAllConnections()(implicit traceContext: TraceContext): Seq[SequencerConnectionX] =
    (lock.exclusive(pool.values.flatten.toSeq))
}

object SequencerConnectionXPoolImpl {
  private final case class BootstrapInfo private (
      synchronizerId: PhysicalSynchronizerId,
      staticParameters: StaticSynchronizerParameters,
  )

  private object BootstrapInfo {
    def fromAttributes(attributes: ConnectionAttributes): BootstrapInfo =
      BootstrapInfo(attributes.physicalSynchronizerId, attributes.staticParameters)
  }
}

class GrpcSequencerConnectionXPoolFactory(
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    authConfig: AuthenticationTokenManagerConfig,
    member: Member,
    clock: Clock,
    crypto: Crypto,
    seedForRandomnessO: Option[Long],
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends SequencerConnectionXPoolFactory
    with NamedLogging {
  import SequencerConnectionXPool.{SequencerConnectionXPoolConfig, SequencerConnectionXPoolError}

  override def create(
      initialConfig: SequencerConnectionXPoolConfig,
      name: String,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool] = {
    val loggerWithPoolName = loggerFactory.append("pool", name)

    val connectionFactory = new GrpcInternalSequencerConnectionXFactory(
      clientProtocolVersions,
      minimumProtocolVersion,
      metrics,
      metricsContext,
      futureSupervisor,
      timeouts,
      loggerWithPoolName,
    )

    for {
      _ <- initialConfig.validate
    } yield {
      new SequencerConnectionXPoolImpl(
        initialConfig,
        connectionFactory,
        clock,
        authConfig,
        member,
        crypto,
        seedForRandomnessO,
        metrics,
        metricsContext,
        futureSupervisor,
        timeouts,
        loggerWithPoolName,
      )
    }
  }

  override def createFromOldConfig(
      sequencerConnections: SequencerConnections,
      expectedPSIdO: Option[PhysicalSynchronizerId],
      tracingConfig: TracingConfig,
      name: String,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      tracingContext: TraceContext,
  ): Either[SequencerConnectionXPoolError, SequencerConnectionXPool] = {
    val poolConfig = SequencerConnectionXPoolConfig.fromSequencerConnections(
      sequencerConnections,
      tracingConfig,
      expectedPSIdO,
    )
    logger.debug(s"poolConfig = $poolConfig")

    create(poolConfig, name)
  }
}
